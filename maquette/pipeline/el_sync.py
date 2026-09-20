"""
============================================================
INGESTION (EL) — remplaçant d'Airbyte dans la maquette
------------------------------------------------------------
Même rôle et mêmes modes de synchronisation qu'un connecteur Airbyte :

  source                        destination          mode Airbyte équivalent
  OLTP PostgreSQL (4 tables)    entrepôt, couche raw  Incremental | Append + Deduped (curseur created_at)
                                                      Full refresh | Overwrite (petites tables)
  Données de référence (CSV)    entrepôt, couche raw  Full refresh | Overwrite
  Événements applicatifs (JSONL) MongoDB              Incremental (curseur = dernier fichier lu)

Extraction incrémentale par created_at, sans CDC (choix documenté au Livrable 5).
Aucune transformation métier ici : on charge le brut, dbt transforme ensuite (ELT).
============================================================
"""
from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
import psycopg2.extensions
from psycopg2.extras import execute_values
from pymongo import MongoClient, ReplaceOne

from pipeline.config import MONGO_DB, env

# Fenêtre de recouvrement : une transaction validée tardivement mais horodatée plus tôt
# que le dernier curseur est rattrapée ; les doublons sont absorbés par l'upsert sur la clé.
LOOKBACK = timedelta(minutes=10)


@dataclass(frozen=True)
class Stream:
    name: str
    mode: str                  # "incremental" | "full_refresh"
    columns: tuple[str, ...]
    primary_key: tuple[str, ...]
    cursor: str | None = None


OLTP_STREAMS = (
    Stream("payment_methods", "full_refresh", ("payment_method_id", "label"), ("payment_method_id",)),
    Stream("merchants", "incremental", ("merchant_id", "name", "country_code", "created_at"),
           ("merchant_id",), "created_at"),
    # Pas d'email : le rôle el_reader n'a pas le droit de le lire (minimisation RGPD)
    Stream("customers", "incremental", ("customer_id", "country_code", "created_at"),
           ("customer_id",), "created_at"),
    # Pas de fraud_score : le score arrive dans l'entrepôt par la couche ml, pas par l'ingestion
    Stream("transactions", "incremental",
           ("transaction_id", "customer_id", "merchant_id", "payment_method_id", "currency_code",
            "amount", "ip_geolocation", "device_type", "status", "created_at"),
           ("transaction_id",), "created_at"),
)

REFERENCE_FILES = (
    Stream("countries", "full_refresh", ("country_code", "name"), ("country_code",)),
    Stream("exchange_rates", "full_refresh", ("currency_code", "rate_date", "rate_to_usd"),
           ("currency_code", "rate_date")),
)

EVENT_COLLECTIONS = {
    "payment_risk_features": "ml_features",
    "session": "user_sessions",
    "feedback": "customer_feedback",
    "log": "logs",
}
DATETIME_FIELDS = {"computed_at", "started_at", "created_at", "ts"}


# ---------------------------------------------------------------- état des flux

def _get_cursor(wh_cur, stream: str) -> str | None:
    wh_cur.execute("SELECT cursor_value FROM raw._sync_state WHERE stream = %s", (stream,))
    row = wh_cur.fetchone()
    return row[0] if row else None


def _save_state(wh_cur, stream: str, mode: str, cursor_value: str | None, rows: int) -> None:
    wh_cur.execute(
        """INSERT INTO raw._sync_state (stream, sync_mode, cursor_value, rows_synced, synced_at)
           VALUES (%s, %s, %s, %s, now())
           ON CONFLICT (stream) DO UPDATE
           SET sync_mode = EXCLUDED.sync_mode, cursor_value = EXCLUDED.cursor_value,
               rows_synced = EXCLUDED.rows_synced, synced_at = now()""",
        (stream, mode, cursor_value, rows),
    )


def _upsert(wh_cur, stream: Stream, rows: list[tuple]) -> None:
    if stream.mode == "full_refresh":
        wh_cur.execute(f"TRUNCATE raw.{stream.name}")
    if not rows:
        return
    cols = ", ".join(stream.columns)
    pk = ", ".join(stream.primary_key)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in stream.columns if c not in stream.primary_key)
    execute_values(
        wh_cur,
        f"INSERT INTO raw.{stream.name} ({cols}) VALUES %s "
        f"ON CONFLICT ({pk}) DO UPDATE SET {updates}, _loaded_at = now()",
        rows,
    )


# ---------------------------------------------------------------- 1. OLTP -> raw

def sync_oltp() -> dict:
    """Copie les nouvelles lignes de l'OLTP vers raw. Toutes les tables sont lues dans
    un même instantané (REPEATABLE READ) : une transaction n'arrive jamais dans raw
    sans son client ou son commerçant. Écriture en une seule transaction côté entrepôt."""
    src = psycopg2.connect(env("OLTP_EL_DSN"))
    src.set_session(isolation_level=psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ, readonly=True)
    dst = psycopg2.connect(env("WH_LOADER_DSN"))
    report = {}
    try:
        with src.cursor() as s_cur, dst.cursor() as d_cur:
            for stream in OLTP_STREAMS:
                cols = ", ".join(stream.columns)
                last = _get_cursor(d_cur, stream.name) if stream.mode == "incremental" else None
                if last is None:
                    s_cur.execute(f"SELECT {cols} FROM oltp.{stream.name}")
                else:
                    s_cur.execute(
                        f"SELECT {cols} FROM oltp.{stream.name} WHERE {stream.cursor} >= %s",
                        (datetime.fromisoformat(last) - LOOKBACK,),
                    )
                rows = s_cur.fetchall()
                _upsert(d_cur, stream, rows)

                new_cursor = last
                if stream.mode == "incremental" and rows:
                    idx = stream.columns.index(stream.cursor)
                    max_value = max(r[idx] for r in rows)
                    if last is None or max_value > datetime.fromisoformat(last):
                        new_cursor = max_value.isoformat()
                _save_state(d_cur, stream.name, stream.mode, new_cursor, len(rows))
                report[stream.name] = len(rows)
        dst.commit()
        src.commit()
    except Exception:
        dst.rollback()
        raise
    finally:
        src.close()
        dst.close()
    print(f"OLTP -> raw : {report}")
    return report


# ---------------------------------------------------------------- 2. Référence -> raw

def sync_reference() -> dict:
    ref_dir = Path(env("REFERENCE_DIR"))
    dst = psycopg2.connect(env("WH_LOADER_DSN"))
    report = {}
    try:
        with dst.cursor() as d_cur:
            for stream in REFERENCE_FILES:
                with (ref_dir / f"{stream.name}.csv").open(encoding="utf-8", newline="") as fh:
                    rows = [tuple(record[c] for c in stream.columns) for record in csv.DictReader(fh)]
                _upsert(d_cur, stream, rows)
                _save_state(d_cur, f"reference.{stream.name}", stream.mode, None, len(rows))
                report[stream.name] = len(rows)
        dst.commit()
    except Exception:
        dst.rollback()
        raise
    finally:
        dst.close()
    print(f"Référence -> raw : {report}")
    return report


# ---------------------------------------------------------------- 3. Événements -> MongoDB

def _parse_dates(doc: dict) -> dict:
    for key, value in doc.items():
        if key in DATETIME_FIELDS and isinstance(value, str):
            doc[key] = datetime.fromisoformat(value)
        elif isinstance(value, list):
            doc[key] = [_parse_dates(v) if isinstance(v, dict) else v for v in value]
    return doc


def sync_events() -> dict:
    """Charge les fichiers d'événements non encore lus dans les collections MongoDB.
    Écritures idempotentes (upsert) : relire un fichier ne crée pas de doublon."""
    events_dir = Path(env("EVENTS_DIR"))
    mongo = MongoClient(env("MONGO_EL_URI"))[MONGO_DB]
    wh = psycopg2.connect(env("WH_LOADER_DSN"))
    totals = {collection: 0 for collection in EVENT_COLLECTIONS.values()}
    files_read = 0
    try:
        with wh.cursor() as cur:
            last_file = _get_cursor(cur, "app_events")
        pending = sorted(p for p in events_dir.glob("events_*.jsonl")
                         if last_file is None or p.name > last_file)

        for path in pending:
            operations: dict[str, list] = {c: [] for c in EVENT_COLLECTIONS.values()}
            with path.open(encoding="utf-8") as fh:
                for line in fh:
                    doc = _parse_dates(json.loads(line))
                    collection = EVENT_COLLECTIONS[doc.pop("event_type")]
                    event_id = doc.pop("event_id")
                    if collection == "ml_features":
                        # une ligne de features par transaction : clé naturelle transaction_id
                        operations[collection].append(
                            ReplaceOne({"transaction_id": doc["transaction_id"]}, doc, upsert=True))
                    else:
                        doc["_id"] = event_id
                        operations[collection].append(ReplaceOne({"_id": event_id}, doc, upsert=True))
            for collection, ops in operations.items():
                if ops:
                    mongo[collection].bulk_write(ops, ordered=False)
                    totals[collection] += len(ops)
            # curseur avancé fichier par fichier : une panne ne fait relire que le fichier en cours
            with wh.cursor() as cur:
                _save_state(cur, "app_events", "incremental", path.name, sum(len(o) for o in operations.values()))
            wh.commit()
            files_read += 1
    finally:
        wh.close()
    report = {"files": files_read, **totals}
    print(f"Événements -> MongoDB : {report}")
    return report
