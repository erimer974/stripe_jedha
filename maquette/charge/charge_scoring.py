"""
============================================================
TEST DE CHARGE — service de scoring (chemin synchrone)
------------------------------------------------------------
Envoie N requêtes /score avec C clients en parallèle et mesure le débit
(requêtes par seconde) et la latence (médiane, p99). On le lance avec
1 exemplaire du service, puis avec 3, pour montrer la montée en charge
horizontale (scale out) d'un service sans état.

Usage (depuis le conteneur app-simulator) :
  python /opt/charge/charge_scoring.py --requests 3000 --clients 24
============================================================
"""
from __future__ import annotations

import argparse
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor

import requests

URL = os.environ.get("SCORING_URL", "http://scoring-api:8000/score")


def _one_call(_: int) -> tuple[float, bool]:
    features = {"amt": round(random.uniform(5, 4000), 2), "geo_distance_km": round(random.uniform(0, 5000), 1),
                "velocity_1h": random.randint(0, 20)}
    started = time.perf_counter()
    try:
        ok = requests.post(URL, json=features, timeout=2).status_code == 200
    except requests.RequestException:
        ok = False
    return (time.perf_counter() - started) * 1000, ok


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", type=int, default=3000)
    parser.add_argument("--clients", type=int, default=24)
    args = parser.parse_args()

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.clients) as pool:
        results = list(pool.map(_one_call, range(args.requests)))
    elapsed = time.perf_counter() - started

    latencies = sorted(ms for ms, ok in results if ok)
    errors = sum(1 for _, ok in results if not ok)
    p = lambda q: latencies[min(len(latencies) - 1, int(q * len(latencies)))] if latencies else 0.0  # noqa: E731
    print(f"{args.requests} requêtes, {args.clients} clients en parallèle : "
          f"{len(latencies) / elapsed:.0f} req/s ; médiane {p(0.5):.0f} ms ; p99 {p(0.99):.0f} ms ; "
          f"erreurs {errors}")


if __name__ == "__main__":
    main()
