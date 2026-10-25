"""
============================================================
CHEMIN SYNCHRONE — service de scoring en ligne (Livrable 7 §3)
------------------------------------------------------------
Appelé par l'API de paiement AVANT l'enregistrement de la transaction :
elle envoie les features du paiement, reçoit un score et une décision.

  POST /score   {"amt": 42.5, "geo_distance_km": 3.1, "velocity_1h": 1}
             -> {"fraud_score": 0.03, "decision": "accept", "model_version": "2", "latency_ms": 4.2}
  GET  /health  -> état du service et version du modèle chargé

Le modèle "champion" du registre MLflow est gardé en mémoire et rechargé
quand l'alias change (promotion d'un nouveau modèle) : pas de redéploiement.
Sans champion, le service répond 503 et l'API de paiement applique sa règle
de repli ; la transaction sera scorée plus tard par la voie lots.

Lancement : gunicorn -w 2 -b 0.0.0.0:8000 pipeline.scoring_api:app
============================================================
"""
from __future__ import annotations

import threading
import time

import pandas as pd
from flask import Flask, jsonify, request

from pipeline import ml_fraud

REFRESH_SECONDS = 30
app = Flask(__name__)

_lock = threading.Lock()
_state = {"model": None, "version": None, "checked_at": 0.0}


def _refresh_model() -> None:
    """Charge le champion s'il a changé. Une erreur du registre garde le modèle courant."""
    try:
        champion = ml_fraud.get_champion()
        if champion is None or champion.version == _state["version"]:
            return
        model = ml_fraud._mlflow().sklearn.load_model(f"models:/{ml_fraud.MODEL_NAME}@{ml_fraud.CHAMPION_ALIAS}")
        with _lock:
            _state.update(model=model, version=str(champion.version))
        print(f"[scoring-api] modèle chargé : version {champion.version}", flush=True)
    except Exception as exc:                          # noqa: BLE001 — le service continue avec l'ancien modèle
        print(f"[scoring-api] registre MLflow injoignable, modèle courant conservé : {exc}", flush=True)


def _refresh_loop() -> None:
    while True:
        _refresh_model()
        _state["checked_at"] = time.time()
        time.sleep(REFRESH_SECONDS)


threading.Thread(target=_refresh_loop, daemon=True).start()


@app.get("/health")
def health():
    return jsonify(status="ok" if _state["model"] is not None else "no_model",
                   model_version=_state["version"])


@app.post("/score")
def score():
    started = time.perf_counter()
    payload = request.get_json(silent=True) or {}
    missing = [f for f in ml_fraud.FEATURES if f not in payload]
    if missing:
        return jsonify(error=f"features manquantes : {missing}"), 400
    with _lock:
        model, version = _state["model"], _state["version"]
    if model is None:
        return jsonify(error="aucun modèle champion disponible"), 503

    try:
        features = pd.DataFrame([{f: float(payload[f]) for f in ml_fraud.FEATURES}])
    except (TypeError, ValueError):
        return jsonify(error="les features doivent être numériques"), 400
    try:
        fraud_score = round(float(model.predict_proba(features)[0, 1]), 4)
    except Exception as exc:                      # noqa: BLE001 — l'API de paiement appliquera sa règle de repli
        app.logger.error("échec du modèle %s : %s", version, exc)
        return jsonify(error=f"échec du modèle {version}"), 500
    # Le modèle signale, il ne refuse pas : au-dessus du seuil, authentification renforcée
    # du client (3-D Secure) et file de revue humaine.
    decision = "challenge" if fraud_score >= ml_fraud.HIGH_RISK else "accept"
    return jsonify(fraud_score=fraud_score, decision=decision, model_version=version,
                   latency_ms=round((time.perf_counter() - started) * 1000, 2))
