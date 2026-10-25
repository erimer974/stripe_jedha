"""
============================================================
VOIE FLUX — consommateurs Kafka (rôle des connecteurs de la cible)
------------------------------------------------------------
Trois consommateurs indépendants, chacun avec son groupe Kafka et le rôle
au moindre privilège de sa destination :

  topics                               destination                 rôle cible
  cdc.oltp.*  (Debezium)               entrepôt, couche raw        Snowpipe Streaming
  events.*, logs.app (application)     MongoDB, 4 collections      connecteur MongoDB
  fraud.scores (service de scoring)    entrepôt, ml.fraud_scores   Snowpipe Streaming

Garantie de livraison : « au moins une fois » + écritures idempotentes
(upsert sur la clé), donc chaque message produit son effet une seule fois.
Le décalage (offset) n'est validé qu'APRÈS l'écriture en base : si le
consommateur s'arrête, il reprend au dernier message non écrit.

Gestion des erreurs (pipeline/errors.py) :
  - message invalide (JSON illisible, champ manquant, type incorrect, refusé
    par la base) : mis en QUARANTAINE avec sa raison, le flux continue ;
  - indisponibilité (base, réseau) : nouvel essai, attente 1, 2, 4… 30 s ;
    au-delà de 5 échecs consécutifs, ALERTE ; une seconde alerte au retour.

Usage : python -m pipeline.stream_processor
============================================================
"""
from __future__ import annotations

import json
import threading
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation

import psycopg2
from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError
from psycopg2.extras import execute_values
from pymongo import MongoClient, ReplaceOne
from pymongo.errors import BulkWriteError

from pipeline.alerting import raise_alert
from pipeline.config import MONGO_DB, env
from pipeline.errors import InvalidRecord, is_transient

BATCH_SIZE = 500          # messages lus avant une écriture groupée
POLL_TIMEOUT_S = 1.0
FAILURES_BEFORE_ALERT = 5
MAX_PAUSE_S = 30

# Colonnes chargées dans raw, par table de l'OLTP, avec leur type attendu
RAW_TABLES = {
    "payment_methods": {"payment_method_id": int, "label": str},
    "merchants": {"merchant_id": int, "name": str, "country_code": str, "created_at": datetime},
    "customers": {"customer_id": int, "country_code": str, "created_at": datetime},     # jamais l'email
    "transactions": {"transaction_id": int, "customer_id": int, "merchant_id": int, "payment_method_id": int,
                     "currency_code": str, "amount": Decimal, "ip_geolocation": str, "device_type": str,
                     "status": str, "created_at": datetime},                         # fraud_score : voie ml
}
PRIMARY_KEYS = {"payment_methods": "payment_method_id", "merchants": "merchant_id",
                "customers": "customer_id", "transactions": "transaction_id"}
NULLABLE = {"ip_geolocation", "device_type", "country_code"}

EVENT_TOPICS = {
    "events.risk_features": "ml_features",
    "events.sessions": "user_sessions",
    "events.feedback": "customer_feedback",
    "logs.app": "logs",
}
DATETIME_FIELDS = {"computed_at", "started_at", "created_at", "ts"}
FEATURES = ("amt", "geo_distance_km", "velocity_1h")

# Erreurs de la base causées par la DONNÉE (et non par une indisponibilité) : définitives
DATA_ERRORS = (psycopg2.DataError, psycopg2.IntegrityError)


# ================================================================ validation (fonctions pures, testées)

def _load_json(value: bytes) -> dict:
    try:
        record = json.loads(value)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidRecord(f"JSON illisible : {exc}") from exc
    if not isinstance(record, dict):
        raise InvalidRecord("le message n'est pas un objet JSON")
    return record


def _typed(field: str, value, expected):
    """Contrôle le type d'un champ ; renvoie la valeur à écrire."""
    if value is None:
        if field in NULLABLE:
            return None
        raise InvalidRecord(f"champ obligatoire vide : {field}")
    try:
        if expected is int:
            if isinstance(value, bool) or int(value) != float(value):
                raise ValueError
            return int(value)
        if expected is Decimal:
            return Decimal(str(value))
        if expected is datetime:
            datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return value
        return str(value)
    except (ValueError, TypeError, InvalidOperation) as exc:
        raise InvalidRecord(f"{field} : valeur invalide {value!r}") from exc


def parse_cdc(topic: str, value: bytes):
    """Message Debezium -> (table, clé, ligne, lsn), ou None pour une suppression (ignorée :
    registre en ajout seul). Lève InvalidRecord si le message ne respecte pas le contrat."""
    table = topic.rsplit(".", 1)[-1]
    if table not in RAW_TABLES:
        raise InvalidRecord(f"table inconnue : {table}")
    record = _load_json(value)
    if record.get("__op") == "d":
        return None
    columns = RAW_TABLES[table]
    row = tuple(_typed(c, record.get(c), t) for c, t in columns.items())
    key = row[list(columns).index(PRIMARY_KEYS[table])]
    return table, key, row, record.get("__source_lsn") or 0


def _parse_dates(doc: dict) -> dict:
    for key, value in doc.items():
        if key in DATETIME_FIELDS and isinstance(value, str):
            try:
                doc[key] = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError as exc:
                raise InvalidRecord(f"{key} : date invalide {value!r}") from exc
        elif isinstance(value, list):
            doc[key] = [_parse_dates(v) if isinstance(v, dict) else v for v in value]
    return doc


def parse_event(topic: str, value: bytes) -> tuple[str, dict, dict]:
    """Événement applicatif -> (collection, filtre d'upsert, document)."""
    collection = EVENT_TOPICS[topic]
    doc = _parse_dates(_load_json(value))
    event_id = doc.pop("event_id", None)
    if collection == "ml_features":                    # même contrat que le $jsonSchema de MongoDB
        features = doc.get("features")
        if not isinstance(doc.get("transaction_id"), int) or not isinstance(features, dict):
            raise InvalidRecord("features : transaction_id entier et objet features obligatoires")
        if any(not isinstance(features.get(f), (int, float)) for f in FEATURES):
            raise InvalidRecord(f"features : {', '.join(FEATURES)} doivent être numériques")
        if not isinstance(doc.get("computed_at"), datetime):
            raise InvalidRecord("features : computed_at obligatoire")
        return collection, {"transaction_id": doc["transaction_id"]}, doc
    if not event_id:
        raise InvalidRecord("event_id manquant")
    doc["_id"] = event_id
    return collection, {"_id": event_id}, doc


def parse_score(value: bytes) -> tuple:
    record = _load_json(value)
    transaction_id = _typed("transaction_id", record.get("transaction_id"), int)
    score = record.get("fraud_score")
    if not isinstance(score, (int, float)) or not 0 <= score <= 1:
        raise InvalidRecord(f"fraud_score hors de [0, 1] : {score!r}")
    return transaction_id, score, _typed("model_version", record.get("model_version"), str)


# ================================================================ quarantaine

class Quarantine:
    """Range les enregistrements refusés dans raw._quarantine, avec leur raison."""

    def __init__(self):
        self.db = _Postgres("WH_LOADER_DSN")

    def store(self, rejected: list[tuple]) -> None:
        """rejected : (message Kafka, raison)."""
        if not rejected:
            return
        rows = [(msg.topic(), f"{msg.partition()}:{msg.offset()}",
                 (msg.value() or b"").decode("utf-8", errors="replace")[:10000], reason[:1000])
                for msg, reason in rejected]
        conn = self.db.get()
        try:
            with conn.cursor() as cur:
                execute_values(cur, "INSERT INTO raw._quarantine (source, location, payload, error) VALUES %s", rows)
            conn.commit()
        except Exception:
            self.db.rollback()
            raise


def _split(batch, parse) -> tuple[list, list]:
    """Sépare les messages valides (message, résultat) des invalides (message, raison)."""
    valid, rejected = [], []
    for msg in batch:
        try:
            valid.append((msg, parse(msg)))
        except InvalidRecord as exc:
            rejected.append((msg, str(exc)))
    return valid, rejected


def _write_rows(conn, sql: str, items: list[tuple]) -> list[tuple]:
    """Écrit (message, ligne) en un lot ; si la base refuse le lot à cause d'une DONNÉE, réécrit
    ligne par ligne pour isoler la ou les lignes fautives. Renvoie les refus (message, raison)."""
    if not items:
        return []
    with conn.cursor() as cur:
        # point de sauvegarde : un refus n'annule que ce lot, pas les tables déjà écrites
        cur.execute("SAVEPOINT un_lot")
        try:
            execute_values(cur, sql, [row for _, row in items])
            cur.execute("RELEASE SAVEPOINT un_lot")
            return []
        except DATA_ERRORS:
            cur.execute("ROLLBACK TO SAVEPOINT un_lot")
    refused = []
    with conn.cursor() as cur:
        for msg, row in items:
            cur.execute("SAVEPOINT une_ligne")
            try:
                execute_values(cur, sql, [row])
                cur.execute("RELEASE SAVEPOINT une_ligne")
            except DATA_ERRORS as exc:
                cur.execute("ROLLBACK TO SAVEPOINT une_ligne")
                refused.append((msg, f"refusé par la base : {str(exc).strip().splitlines()[0]}"))
    return refused


# ================================================================ boucle commune

def _consumer(group: str, topics: list[str]) -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": env("KAFKA_BOOTSTRAP_SERVERS"),
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,         # validation manuelle, après écriture
        # un topic créé après le démarrage (premier message de Debezium) est pris en compte en 5 s
        "topic.metadata.refresh.interval.ms": 5000,
    })
    consumer.subscribe(topics)
    return consumer


def _poll_batch(consumer: Consumer) -> list:
    """Lit jusqu'à BATCH_SIZE messages ; ignore la fin de partition, remonte les vraies erreurs."""
    messages = []
    for msg in consumer.consume(num_messages=BATCH_SIZE, timeout=POLL_TIMEOUT_S):
        if msg.error():
            # fin de partition, ou topic pas encore créé (Debezium ne crée un topic qu'au premier changement)
            if msg.error().code() in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                continue
            raise ConnectionError(f"erreur Kafka : {msg.error()}")
        if msg.value() is not None:
            messages.append(msg)
    return messages


def _run_forever(name: str, consumer: Consumer, handle_batch) -> None:
    """Lire un lot, l'écrire, puis seulement valider les offsets. Une erreur n'avance pas
    l'offset : le lot est relu après une pause qui double à chaque échec (plafond 30 s)."""
    pause, failures = 1, 0
    while True:
        try:
            batch = _poll_batch(consumer)
            if batch:
                started = time.perf_counter()
                written, quarantined = handle_batch(batch)
                consumer.commit(asynchronous=False)
                elapsed_ms = (time.perf_counter() - started) * 1000
                suffix = f", {quarantined} en quarantaine" if quarantined else ""
                print(f"[{name}] {written} messages écrits{suffix} en {elapsed_ms:.0f} ms", flush=True)
                if quarantined:
                    raise_alert(name, "WARNING", f"{quarantined} message(s) mis en quarantaine (raw._quarantine)")
            if failures >= FAILURES_BEFORE_ALERT:
                raise_alert(name, "INFO", f"rétabli après {failures} échecs consécutifs")
            pause, failures = 1, 0
        except Exception as exc:                      # noqa: BLE001 — on journalise et on réessaie
            failures += 1
            kind = "indisponibilité" if is_transient(exc) else "erreur inattendue"
            print(f"[{name}] {kind}, nouvel essai dans {pause} s ({failures}) : {exc}", flush=True)
            if failures == FAILURES_BEFORE_ALERT:
                raise_alert(name, "CRITICAL", f"{failures} échecs consécutifs ({kind}) : {exc}")
            time.sleep(pause)
            pause = min(pause * 2, MAX_PAUSE_S)       # attente exponentielle plafonnée
            _rewind(consumer)


def _rewind(consumer: Consumer) -> None:
    """Revient au dernier offset validé pour relire le lot non écrit."""
    assignment = consumer.assignment()
    if assignment:
        for tp in consumer.committed(assignment, timeout=10):
            if tp.offset < 0:                         # rien encore validé : depuis le début
                tp.offset = OFFSET_BEGINNING
            consumer.seek(tp)


class _Postgres:
    """Connexion PostgreSQL rouverte automatiquement si la base a redémarré."""

    def __init__(self, dsn_variable: str):
        self.dsn = env(dsn_variable)
        self.conn = None

    def get(self):
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(self.dsn, connect_timeout=5)
        return self.conn

    def rollback(self) -> None:
        try:
            if self.conn is not None and not self.conn.closed:
                self.conn.rollback()
        except psycopg2.Error:
            self.conn = None                          # connexion perdue : rouverte au prochain lot


def _with_rollback(db: _Postgres, handle):
    """Annule la transaction en cours si l'écriture échoue, avant le nouvel essai."""
    def wrapped(batch):
        try:
            return handle(batch)
        except Exception:
            db.rollback()
            raise
    return wrapped


# ================================================================ 1. CDC -> raw

def _cdc_sink() -> None:
    db = _Postgres("WH_LOADER_DSN")
    quarantine = Quarantine()
    consumer = _consumer("sink-raw", [f"cdc.oltp.{t}" for t in RAW_TABLES])

    def handle(batch) -> tuple[int, int]:
        valid, rejected = _split(batch, lambda m: parse_cdc(m.topic(), m.value()))
        by_table: dict[str, dict] = {t: {} for t in RAW_TABLES}
        last_lsn: dict[str, int] = {}
        for msg, parsed in valid:
            if parsed is None:                        # suppression : registre en ajout seul
                continue
            table, key, row, lsn = parsed
            by_table[table][key] = (msg, row)         # la dernière version gagne
            last_lsn[table] = lsn
        wh = db.get()
        written = 0
        for table, items in by_table.items():
            if not items:
                continue
            cols = list(RAW_TABLES[table])
            pk = PRIMARY_KEYS[table]
            updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != pk)
            sql = (f"INSERT INTO raw.{table} ({', '.join(cols)}) VALUES %s "
                   f"ON CONFLICT ({pk}) DO UPDATE SET {updates}, _loaded_at = now()")
            refused = _write_rows(wh, sql, list(items.values()))
            rejected += refused
            written += len(items) - len(refused)
            with wh.cursor() as cur:
                cur.execute(
                    """INSERT INTO raw._sync_state (stream, sync_mode, cursor_value, rows_synced, synced_at)
                       VALUES (%s, 'cdc', %s, %s, now())
                       ON CONFLICT (stream) DO UPDATE SET sync_mode = 'cdc', cursor_value = EXCLUDED.cursor_value,
                           rows_synced = EXCLUDED.rows_synced, synced_at = now()""",
                    (table, f"lsn={last_lsn.get(table)}", len(items)))
        wh.commit()
        quarantine.store(rejected)
        return written, len(rejected)

    _run_forever("cdc->raw", consumer, _with_rollback(db, handle))


# ================================================================ 2. événements -> MongoDB

def _events_sink() -> None:
    mongo = MongoClient(env("MONGO_EL_URI"), serverSelectionTimeoutMS=5000)[MONGO_DB]
    quarantine = Quarantine()
    consumer = _consumer("sink-mongodb", list(EVENT_TOPICS))

    def handle(batch) -> tuple[int, int]:
        valid, rejected = _split(batch, lambda m: parse_event(m.topic(), m.value()))
        by_collection: dict[str, list] = {}
        for msg, (collection, key, doc) in valid:
            by_collection.setdefault(collection, []).append((msg, ReplaceOne(key, doc, upsert=True)))
        written = 0
        for collection, items in by_collection.items():
            try:
                mongo[collection].bulk_write([op for _, op in items], ordered=False)
                written += len(items)
            except BulkWriteError as exc:
                # documents refusés par MongoDB (validation $jsonSchema...) : en quarantaine ;
                # les autres opérations du lot ont été appliquées (ordered=False)
                errors = exc.details.get("writeErrors", [])
                for err in errors:
                    rejected.append((items[err["index"]][0], f"refusé par MongoDB : {err.get('errmsg', '')[:300]}"))
                written += len(items) - len(errors)
        quarantine.store(rejected)
        return written, len(rejected)

    _run_forever("events->mongodb", consumer, handle)


# ================================================================ 3. scores -> ml

def _scores_sink() -> None:
    db = _Postgres("WH_ML_DSN")
    quarantine = Quarantine()
    consumer = _consumer("sink-ml-scores", ["fraud.scores"])

    def handle(batch) -> tuple[int, int]:
        valid, rejected = _split(batch, lambda m: parse_score(m.value()))
        by_tx = {row[0]: (msg, row) for msg, row in valid}
        wh = db.get()
        refused = _write_rows(wh, "INSERT INTO ml.fraud_scores (transaction_id, fraud_score, model_version) "
                                  "VALUES %s ON CONFLICT (transaction_id) DO NOTHING", list(by_tx.values()))
        wh.commit()
        rejected += refused
        quarantine.store(rejected)
        return len(by_tx) - len(refused), len(rejected)

    _run_forever("scores->ml", consumer, _with_rollback(db, handle))


def main() -> None:
    sinks = {"cdc->raw": _cdc_sink, "events->mongodb": _events_sink, "scores->ml": _scores_sink}
    threads = [threading.Thread(target=fn, name=name, daemon=True) for name, fn in sinks.items()]
    for t in threads:
        t.start()
    print(f"[stream-processor] consommateurs démarrés : {', '.join(sinks)}", flush=True)
    while all(t.is_alive() for t in threads):
        time.sleep(5)
    # un consommateur s'est arrêté sur une erreur non récupérable : le conteneur redémarre (restart policy)
    raise_alert("stream-processor", "CRITICAL", "un consommateur s'est arrêté : redémarrage du conteneur")
    raise SystemExit("[stream-processor] un consommateur s'est arrêté")


if __name__ == "__main__":
    main()
