"""
============================================================
SOURCES du schéma : application Stripe simulée (hors pipeline)
------------------------------------------------------------
Joue le rôle de l'application réelle, qui vit indépendamment du pipeline :
  - l'API de paiement écrit clients, commerçants et transactions dans
    l'OLTP PostgreSQL (rôle payment_service : SELECT / INSERT uniquement) ;
  - l'application émet ses événements (sessions, avis clients, logs,
    features de risque) dans des fichiers JSON Lines.

Le pipeline Airflow vient ensuite chercher ces deux sources, comme le ferait
Airbyte : le simulateur n'écrit ni dans l'entrepôt ni dans MongoDB.

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
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values


def env(name: str) -> str:
    """Aucun secret dans le code : la connexion vient de l'environnement.
    docker compose la fournit à partir du fichier .env (voir .env.example)."""
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Variable d'environnement manquante : {name}")
    return value


EVENTS_DIR = Path(os.environ.get("EVENTS_DIR", "./app_events"))
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
NEW_CUSTOMER_PROBABILITY = 0.3   # par lot : la base clients grossit, l'extraction incrémentale a du travail


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


def _write_events_file(events: list[dict]) -> Path:
    """Écriture atomique : fichier .tmp puis renommage. Le pipeline ne lit que les
    fichiers .jsonl complets, nommés par horodatage (ordre de lecture = ordre d'émission)."""
    EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    final_path = EVENTS_DIR / f"events_{stamp}.jsonl"
    tmp_path = final_path.with_suffix(".jsonl.tmp")
    with tmp_path.open("w", encoding="utf-8") as fh:
        for event in events:
            fh.write(json.dumps(event, default=lambda v: v.isoformat()) + "\n")
    os.replace(tmp_path, final_path)
    return final_path


def generate_batch(n: int, drift: bool = False) -> int:
    conn = psycopg2.connect(env("OLTP_APP_DSN"))
    try:
        # --- 1. API de paiement : une transaction ACID pour tout le lot ---
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
                cur.execute(
                    """INSERT INTO transactions
                           (customer_id, merchant_id, payment_method_id, currency_code,
                            amount, ip_geolocation, device_type, status)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       RETURNING transaction_id""",
                    (tx["customer_id"], random.choice(merchant_ids), random.choice(payment_method_ids),
                     random.choice(CURRENCIES), tx["amount"],
                     f"{random.randint(1, 223)}.{random.randint(0, 255)}.0.{random.randint(1, 254)}",
                     tx["device"], tx["status"]),
                )
                tx["transaction_id"] = cur.fetchone()[0]
                created.append(tx)
    finally:
        conn.close()

    # --- 2. Événements applicatifs, reliés à l'OLTP par transaction_id / customer_id ---
    now = datetime.now(timezone.utc)
    events: list[dict] = []
    for tx in created:
        events.append({
            "event_type": "payment_risk_features",
            "event_id": uuid.uuid4().hex,
            "transaction_id": tx["transaction_id"],
            "features": {
                "amt": tx["amount"],
                "geo_distance_km": tx["geo_distance"],
                "velocity_1h": tx["velocity"],
            },
            # Étiquette d'apprentissage. Dans la réalité, elle arrive plus tard
            # (rétrofacturations, enquêtes) ; la maquette la fournit immédiatement.
            "is_fraud": tx["is_fraud"],
            "computed_at": now,
        })
        events.append({
            "event_type": "session",
            "event_id": f"sess_{uuid.uuid4().hex[:12]}",
            "customer_id": tx["customer_id"],
            "device": {"type": tx["device"], "os": "iOS" if tx["device"] == "mobile" else "Windows"},
            "started_at": now - timedelta(minutes=random.randint(1, 30)),
            "events": [   # EMBEDDING : les clics vivent dans la session
                {"type": "click", "page": "/checkout", "ts": now - timedelta(minutes=2)},
                {"type": "click", "page": "/pay", "ts": now},
            ],
        })
        if random.random() < 0.3:
            events.append({
                "event_type": "feedback",
                "event_id": uuid.uuid4().hex,
                "customer_id": tx["customer_id"],
                "rating": random.randint(1, 5),
                "text": random.choice(["Paiement rapide", "RAS", "Interface confuse", "Très satisfait"]),
                "created_at": now,
            })
        if tx["status"] == "failed" and random.random() < 0.5:
            events.append({
                "event_type": "log",
                "event_id": uuid.uuid4().hex,
                "level": "ERROR",
                "service": "payment-api",
                "message": "payment declined by risk engine or bank gateway",
                "transaction_id": tx["transaction_id"],
                "ts": now,
            })

    path = _write_events_file(events)
    print(f"[app-simulator] {len(created)} transactions OLTP, {len(events)} événements -> {path.name}"
          + (" (mode dérive)" if drift else ""), flush=True)
    return len(created)


def main() -> None:
    parser = argparse.ArgumentParser(description="Application Stripe simulée (sources du pipeline)")
    parser.add_argument("--once", action="store_true", help="générer un seul lot puis s'arrêter")
    parser.add_argument("--n", type=int, default=BATCH_SIZE, help="transactions par lot")
    parser.add_argument("--drift", action="store_true", help="modifier le comportement (démo de dérive)")
    args = parser.parse_args()

    while True:
        try:
            generate_batch(args.n, drift=args.drift)
        except psycopg2.OperationalError as exc:
            if args.once:
                raise
            print(f"[app-simulator] OLTP indisponible, nouvel essai au prochain cycle : {exc}", flush=True)
        if args.once:
            break
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
