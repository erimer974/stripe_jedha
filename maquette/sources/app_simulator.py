"""
============================================================
SOURCES du schéma : application Stripe simulée (hors pipeline)
------------------------------------------------------------
Joue le rôle de l'application réelle, qui vit indépendamment du pipeline :
  - CHEMIN SYNCHRONE : pour chaque paiement, l'API calcule les features,
    appelle le service de scoring (délai maximal 300 ms), puis enregistre la
    transaction AVEC son score dans l'OLTP PostgreSQL (rôle payment_service :
    SELECT / INSERT uniquement). Service indisponible : règle de repli, score
    vide, rattrapé plus tard par la voie lots ;
  - VOIE FLUX : après validation, l'application publie ses événements dans
    Kafka (features de risque, score, sessions, avis, journaux).

La capture des changements (Debezium) lit ensuite l'OLTP de son côté : le
simulateur n'écrit ni dans l'entrepôt ni dans MongoDB.

Usage :
  python app_simulator.py                    # boucle continue (service docker)
  python app_simulator.py --once --n 300     # un seul lot
  python app_simulator.py --once --n 400 --drift   # lot au comportement modifié (démo de dérive)
============================================================
"""
from __future__ import annotations

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

import psycopg2
import requests
from confluent_kafka import Producer
from psycopg2.extras import execute_values


def env(name: str) -> str:
    """Aucun secret dans le code : la connexion vient de l'environnement.
    docker compose la fournit à partir du fichier .env (voir .env.example)."""
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Variable d'environnement manquante : {name}")
    return value


def _alert(severity: str, message: str) -> None:
    """Alerte de l'API de paiement : journal + warehouse.audit.alerts (rôle alerter). Ne bloque jamais
    un paiement : si l'entrepôt est indisponible, l'alerte reste dans le journal."""
    print(f"[app-simulator] ALERTE {severity} : {message}", flush=True)
    dsn = os.environ.get("ALERTS_DSN")
    if not dsn:
        return
    try:
        conn = psycopg2.connect(dsn, connect_timeout=2)
        with conn, conn.cursor() as cur:
            cur.execute("INSERT INTO audit.alerts (severity, source, message) VALUES (%s, 'api-paiement', %s)",
                        (severity, message))
        conn.close()
    except psycopg2.Error:
        pass


SCORING_URL = os.environ.get("SCORING_URL", "http://scoring-api:8000/score")
SCORING_TIMEOUT_S = 0.3
INTERVAL_SECONDS = int(os.environ.get("SIMULATOR_INTERVAL_SECONDS", "30"))
BATCH_SIZE = int(os.environ.get("SIMULATOR_BATCH_SIZE", "20"))

COUNTRY_CODES = ["US", "FR", "DE", "GB", "JP"]
CURRENCIES = ["USD", "EUR", "GBP", "JPY"]
MERCHANT_NAMES = [
    "NordShop", "PixelMart", "GreenGrocer", "UrbanThreads", "ByteBazaar",
    "SunnySupplies", "QuickCart", "BlueOcean Goods", "TechNest", "PaperTrail Co.",
]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
FRAUD_RATE = 0.08
NEW_CUSTOMER_PROBABILITY = 0.3   # par lot : la base clients grossit, la capture des changements a du travail


def _ensure_customers_and_merchants(cur) -> None:
    cur.execute("SELECT count(*) FROM customers")
    if cur.fetchone()[0] < 30:
        execute_values(
            cur, "INSERT INTO customers (email, country_code) VALUES %s",
            [(f"customer_{uuid.uuid4().hex[:10]}@example.com", random.choice(COUNTRY_CODES))
             for _ in range(30)],
        )
    cur.execute("SELECT count(*) FROM merchants")
    if cur.fetchone()[0] == 0:
        execute_values(
            cur, "INSERT INTO merchants (name, country_code) VALUES %s",
            [(name, random.choice(COUNTRY_CODES)) for name in MERCHANT_NAMES],
        )
    if random.random() < NEW_CUSTOMER_PROBABILITY:
        cur.execute(
            "INSERT INTO customers (email, country_code) VALUES (%s, %s)",
            (f"customer_{uuid.uuid4().hex[:10]}@example.com", random.choice(COUNTRY_CODES)),
        )


def _draw_transaction(drift: bool) -> dict:
    """Profil des transactions. Les fraudes sont typées pour que le modèle ait un
    signal à apprendre. En mode --drift, les fraudeurs changent de stratégie
    (petits montants, forte vélocité) et les clients légitimes voyagent davantage :
    la distribution des features se déplace, ce que détecte le contrôle de dérive."""
    is_fraud = random.random() < FRAUD_RATE
    if not drift:
        amount = random.uniform(500, 4000) if is_fraud else random.uniform(5, 500)
        velocity = random.randint(5, 20) if is_fraud else random.randint(0, 3)
        geo_distance = random.uniform(500, 5000) if is_fraud else random.uniform(0, 50)
    else:
        amount = random.uniform(5, 300) if is_fraud else random.uniform(5, 800)
        velocity = random.randint(3, 25) if is_fraud else random.randint(0, 6)
        geo_distance = random.uniform(40, 800) if is_fraud else random.uniform(0, 400)
    return {
        "is_fraud": is_fraud,
        "amount": round(amount, 2),
        "velocity": velocity,
        "geo_distance": round(geo_distance, 1),
        "device": random.choice(DEVICE_TYPES),
        "status": "failed" if is_fraud or random.random() < 0.05
                  else random.choice(["succeeded", "succeeded", "succeeded", "refunded"]),
    }


# ---------------------------------------------------------------- chemin synchrone : scoring

class CircuitBreaker:
    """Disjoncteur : après FAILURES_TO_OPEN échecs consécutifs, le service n'est plus appelé
    pendant OPEN_SECONDS. Les paiements passent directement par la règle de repli au lieu
    d'attendre un service en panne (le délai maximal ne couvre pas, par exemple, la recherche
    DNS d'un service arrêté). Passé ce délai, un appel d'essai referme le circuit s'il réussit."""
    FAILURES_TO_OPEN = 3
    OPEN_SECONDS = 30

    def __init__(self):
        self.failures = 0
        self.opened_at: float | None = None

    def allows_call(self) -> bool:
        return self.opened_at is None or time.monotonic() - self.opened_at >= self.OPEN_SECONDS

    def record(self, success: bool) -> None:
        if success:
            if self.opened_at is not None:
                _alert("INFO", "service de scoring rétabli : circuit refermé")
            self.failures, self.opened_at = 0, None
            return
        self.failures += 1
        if self.failures >= self.FAILURES_TO_OPEN and (self.opened_at is None or self.allows_call()):
            first_opening = self.opened_at is None
            self.opened_at = time.monotonic()
            message = f"service de scoring en échec : circuit ouvert {self.OPEN_SECONDS} s, décisions par règle de repli"
            if first_opening:
                _alert("WARNING", message)                 # une alerte par panne, pas à chaque essai
            else:
                print(f"[app-simulator] {message}", flush=True)


SCORING_CIRCUIT = CircuitBreaker()


def _score(features: dict) -> dict | None:
    """Appelle le service de scoring. Délai maximal 300 ms : au-delà, si le service répond une
    erreur ou si le disjoncteur est ouvert, l'API applique sa règle de repli et la transaction
    passe sans score."""
    if not SCORING_CIRCUIT.allows_call():
        return None
    try:
        response = requests.post(SCORING_URL, json=features, timeout=SCORING_TIMEOUT_S)
        if response.status_code == 200:
            SCORING_CIRCUIT.record(success=True)
            return response.json()
        # 503 = pas encore de modèle champion : le service répond vite, ce n'est pas une panne
        SCORING_CIRCUIT.record(success=response.status_code == 503)
    except requests.RequestException:
        SCORING_CIRCUIT.record(success=False)
    return None


def _fallback_decision(features: dict) -> str:
    """Règle de repli quand le modèle est indisponible : montant élevé ou forte vélocité."""
    return "challenge" if features["amt"] > 1000 or features["velocity_1h"] > 5 else "accept"


def _percentile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * len(ordered)))] if ordered else 0.0


# ---------------------------------------------------------------- voie flux : publication Kafka

def _producer() -> Producer:
    # acks=all + idempotence : un message confirmé est écrit sur toutes les répliques, sans doublon
    return Producer({"bootstrap.servers": env("KAFKA_BOOTSTRAP_SERVERS"),
                     "acks": "all", "enable.idempotence": True, "linger.ms": 20})


def _publish(producer: Producer, events: list[tuple[str, str, dict]]) -> int:
    """Publie (topic, clé, événement). La clé fixe la partition : tous les événements
    d'une même transaction ou d'un même client restent dans l'ordre."""
    failures = []

    def on_delivery(err, msg):
        if err is not None:
            failures.append(err)

    for topic, key, event in events:
        producer.produce(topic, key=key, value=json.dumps(event, default=lambda v: v.isoformat()),
                         on_delivery=on_delivery)
        producer.poll(0)
    remaining = producer.flush(30)
    if failures or remaining:
        _alert("WARNING", f"{len(failures) + remaining} événement(s) non confirmé(s) par Kafka")
    return len(events) - len(failures) - remaining


def generate_batch(n: int, producer: Producer, drift: bool = False, quiet: bool = False) -> list[int]:
    """Enregistre n paiements dans une transaction ACID et publie leurs événements.
    Retourne les transaction_id validés (utilisé par la sonde de bascule)."""
    conn = psycopg2.connect(env("OLTP_APP_DSN"))
    latencies: list[float] = []
    decisions = {"accept": 0, "challenge": 0, "fallback": 0}
    try:
        # --- 1. API de paiement : scoring avant enregistrement, une transaction ACID pour le lot ---
        with conn, conn.cursor() as cur:
            cur.execute("SET search_path TO oltp")
            _ensure_customers_and_merchants(cur)
            cur.execute("SELECT customer_id FROM customers")
            customer_ids = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT merchant_id FROM merchants")
            merchant_ids = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT payment_method_id FROM payment_methods")
            payment_method_ids = [r[0] for r in cur.fetchall()]

            created = []
            for _ in range(n):
                tx = _draw_transaction(drift)
                tx["customer_id"] = random.choice(customer_ids)
                tx["features"] = {"amt": tx["amount"], "geo_distance_km": tx["geo_distance"],
                                  "velocity_1h": tx["velocity"]}
                started = time.perf_counter()
                tx["score"] = _score(tx["features"])
                latencies.append((time.perf_counter() - started) * 1000)
                if tx["score"] is None:
                    decisions["fallback"] += 1
                    tx["decision"] = _fallback_decision(tx["features"])
                else:
                    decisions[tx["score"]["decision"]] += 1
                    tx["decision"] = tx["score"]["decision"]
                cur.execute(
                    """INSERT INTO transactions
                           (customer_id, merchant_id, payment_method_id, currency_code,
                            amount, ip_geolocation, device_type, status, fraud_score)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                       RETURNING transaction_id""",
                    (tx["customer_id"], random.choice(merchant_ids), random.choice(payment_method_ids),
                     random.choice(CURRENCIES), tx["amount"],
                     f"{random.randint(1, 223)}.{random.randint(0, 255)}.0.{random.randint(1, 254)}",
                     tx["device"], tx["status"],
                     tx["score"]["fraud_score"] if tx["score"] else None),
                )
                tx["transaction_id"] = cur.fetchone()[0]
                created.append(tx)
    finally:
        conn.close()

    # --- 2. Événements publiés après validation, reliés à l'OLTP par transaction_id / customer_id ---
    now = datetime.now(timezone.utc)
    events: list[tuple[str, str, dict]] = []
    for tx in created:
        tx_key = str(tx["transaction_id"])
        events.append(("events.risk_features", tx_key, {
            "event_id": uuid.uuid4().hex,
            "transaction_id": tx["transaction_id"],
            "features": tx["features"],
            "decision": tx["decision"],
            # Étiquette d'apprentissage. Dans la réalité, elle arrive plus tard
            # (rétrofacturations, enquêtes) ; la maquette la fournit immédiatement.
            "is_fraud": tx["is_fraud"],
            "computed_at": now,
        }))
        if tx["score"]:
            events.append(("fraud.scores", tx_key, {
                "transaction_id": tx["transaction_id"],
                "fraud_score": tx["score"]["fraud_score"],
                "model_version": tx["score"]["model_version"],
                "scored_at": now,
            }))
        events.append(("events.sessions", str(tx["customer_id"]), {
            "event_id": f"sess_{uuid.uuid4().hex[:12]}",
            "customer_id": tx["customer_id"],
            "device": {"type": tx["device"], "os": "iOS" if tx["device"] == "mobile" else "Windows"},
            "started_at": now - timedelta(minutes=random.randint(1, 30)),
            "events": [   # EMBEDDING : les clics vivent dans la session
                {"type": "click", "page": "/checkout", "ts": now - timedelta(minutes=2)},
                {"type": "click", "page": "/pay", "ts": now},
            ],
        }))
        if random.random() < 0.3:
            events.append(("events.feedback", str(tx["customer_id"]), {
                "event_id": uuid.uuid4().hex,
                "customer_id": tx["customer_id"],
                "rating": random.randint(1, 5),
                "text": random.choice(["Paiement rapide", "RAS", "Interface confuse", "Très satisfait"]),
                "created_at": now,
            }))
        if tx["status"] == "failed" and random.random() < 0.5:
            events.append(("logs.app", tx_key, {
                "event_id": uuid.uuid4().hex,
                "level": "ERROR",
                "service": "payment-api",
                "message": "payment declined by risk engine or bank gateway",
                "transaction_id": tx["transaction_id"],
                "ts": now,
            }))

    published = _publish(producer, events)
    if quiet:
        return [tx["transaction_id"] for tx in created]
    print(f"[app-simulator] {len(created)} transactions OLTP, {published} événements Kafka ; "
          f"décisions {decisions} ; scoring p50 {_percentile(latencies, 0.5):.0f} ms, "
          f"p99 {_percentile(latencies, 0.99):.0f} ms"
          + (" (mode dérive)" if drift else ""), flush=True)
    return [tx["transaction_id"] for tx in created]


def main() -> None:
    parser = argparse.ArgumentParser(description="Application Stripe simulée (sources du pipeline)")
    parser.add_argument("--once", action="store_true", help="générer un seul lot puis s'arrêter")
    parser.add_argument("--n", type=int, default=BATCH_SIZE, help="transactions par lot")
    parser.add_argument("--drift", action="store_true", help="modifier le comportement (démo de dérive)")
    args = parser.parse_args()

    producer = _producer()
    while True:
        try:
            generate_batch(args.n, producer, drift=args.drift)
        except psycopg2.OperationalError as exc:
            if args.once:
                raise
            print(f"[app-simulator] OLTP indisponible, nouvel essai au prochain cycle : {exc}", flush=True)
        if args.once:
            break
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
