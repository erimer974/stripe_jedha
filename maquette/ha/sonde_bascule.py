"""
============================================================
SONDE DE BASCULE — mesurer le RPO et le RTO réels (Livrable 5 §9)
------------------------------------------------------------
Pendant DUREE secondes, enregistre des paiements un par un par la vraie API
simulée (scoring compris), à travers le routeur, et note chaque paiement dont
la validation a été confirmée. Pendant ce temps, on provoque la panne du
primaire (ha/test_bascule.sh). À la fin :
  - RPO : les paiements confirmés sont-ils TOUS dans le nouveau primaire ?
  - RTO : plus longue interruption des écritures vue par l'API.

Usage (dans le conteneur app-simulator) :
  python /opt/ha/sonde_bascule.py --duree 70 --sortie /tmp/sonde.json
============================================================
"""
from __future__ import annotations

import argparse
import json
import sys
import time

sys.path.insert(0, "/opt/app")
import app_simulator  # noqa: E402  (l'API de paiement simulée)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duree", type=int, default=70)
    parser.add_argument("--sortie", default="/tmp/sonde.json")
    args = parser.parse_args()

    producer = app_simulator._producer()
    confirmed: list[int] = []
    last_success = time.time()
    longest_gap = 0.0
    failures = 0
    end = time.time() + args.duree
    while time.time() < end:
        try:
            ids = app_simulator.generate_batch(1, producer, quiet=True)
            now = time.time()
            longest_gap = max(longest_gap, now - last_success)
            last_success = now
            confirmed += ids
        except Exception:                      # noqa: BLE001 — pendant la bascule, l'API réessaie
            failures += 1
            time.sleep(0.2)

    result = {"confirmes": len(confirmed), "ids": confirmed, "echecs_pendant_bascule": failures,
              "plus_longue_interruption_s": round(longest_gap, 1)}
    with open(args.sortie, "w", encoding="utf-8") as fh:
        json.dump(result, fh)
    print(f"[sonde] {len(confirmed)} paiements confirmés ; {failures} tentatives refusées pendant la "
          f"bascule ; plus longue interruption des écritures : {longest_gap:.1f} s", flush=True)


if __name__ == "__main__":
    main()
