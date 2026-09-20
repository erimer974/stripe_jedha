"""
============================================================
VALORISATION — modèle de fraude (Livrable 7, modèle réduit)
------------------------------------------------------------
Deux boucles distinctes, comme sur le schéma :

  1. Scoring (DAG stripe_pipeline, toutes les 5 min, APRÈS dbt test)
     features lues dans MongoDB -> modèle "champion" du registre MLflow
     -> fraud_score écrit dans l'entrepôt (ml.fraud_scores, repris par dbt
        dans analytics au passage suivant) ET réécrit dans l'OLTP.

  2. Entraînement continu (DAG stripe_model_retraining, quotidien)
     contrôle de dérive (PSI, rôle d'Evidently) -> entraînement d'un
     candidat -> comparaison au champion sur les mêmes données récentes
     -> promotion si au moins aussi bon.

Modèle explicable (RandomForest, importance des variables tracée), classes
déséquilibrées compensées par class_weight="balanced".
============================================================
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pymongo import MongoClient

from pipeline.config import MONGO_DB, env

FEATURES = ["amt", "geo_distance_km", "velocity_1h"]
MODEL_NAME = "stripe_fraud"
CHAMPION_ALIAS = "champion"
PSI_THRESHOLD = 0.2          # seuil usuel : > 0.2 = dérive significative
MIN_TRAINING_ROWS = 200
MIN_FRAUD_CASES = 10
RECENT_WINDOW = 500          # nombre de transactions récentes comparées à la référence
HIGH_RISK = 0.9


class NotEnoughData(Exception):
    """Levée quand le volume de données ne permet pas un entraînement fiable."""


# ---------------------------------------------------------------- accès MLflow

def _mlflow():
    import mlflow
    mlflow.set_tracking_uri(env("MLFLOW_TRACKING_URI"))
    return mlflow


def get_champion():
    """Version du modèle portant l'alias "champion", ou None."""
    from mlflow.exceptions import MlflowException
    mlflow = _mlflow()
    try:
        return mlflow.MlflowClient().get_model_version_by_alias(MODEL_NAME, CHAMPION_ALIAS)
    except MlflowException:
        return None


# ---------------------------------------------------------------- features (MongoDB)

def _features_collection():
    return MongoClient(env("MONGO_ML_URI"))[MONGO_DB]["ml_features"]


def load_labeled_features(limit: int = 20000) -> pd.DataFrame:
    docs = list(
        _features_collection()
        .find({"is_fraud": {"$ne": None}}, {"_id": 0})
        .sort("computed_at", -1)
        .limit(limit)
    )
    if not docs:
        return pd.DataFrame(columns=["transaction_id", *FEATURES, "is_fraud", "computed_at"])
    df = pd.DataFrame([
        {"transaction_id": d["transaction_id"], **d["features"],
         "is_fraud": int(d["is_fraud"]), "computed_at": d["computed_at"]}
        for d in docs
    ])
    return df.sort_values("computed_at").reset_index(drop=True)


# ---------------------------------------------------------------- dérive (rôle d'Evidently)

def _reference_profile(df: pd.DataFrame) -> dict:
    """Profil de référence : déciles de chaque feature sur les données d'entraînement."""
    profile = {}
    for feature in FEATURES:
        values = df[feature].to_numpy(dtype=float)
        inner_edges = np.unique(np.quantile(values, np.linspace(0, 1, 11))[1:-1])
        counts = np.bincount(np.searchsorted(inner_edges, values, side="right"),
                             minlength=len(inner_edges) + 1)
        profile[feature] = {"inner_edges": inner_edges.tolist(),
                            "proportions": (counts / len(values)).tolist()}
    return profile


def _psi(reference: dict, values: np.ndarray) -> float:
    """Population Stability Index entre la distribution de référence et la distribution récente."""
    inner_edges = np.asarray(reference["inner_edges"])
    counts = np.bincount(np.searchsorted(inner_edges, values, side="right"),
                         minlength=len(inner_edges) + 1)
    actual = np.clip(counts / len(values), 1e-4, None)
    expected = np.clip(np.asarray(reference["proportions"]), 1e-4, None)
    return float(np.sum((actual - expected) * np.log(actual / expected)))


def check_drift() -> dict:
    """Décide s'il faut réentraîner : "bootstrap" (aucun champion), "drift" ou "stable"."""
    champion = get_champion()
    if champion is None:
        return {"decision": "bootstrap"}

    mlflow = _mlflow()
    reference = mlflow.artifacts.load_dict(f"runs:/{champion.run_id}/reference_profile.json")
    recent = pd.DataFrame([
        d["features"] for d in
        _features_collection().find({}, {"_id": 0, "features": 1}).sort("computed_at", -1).limit(RECENT_WINDOW)
    ])
    if len(recent) < 100:
        return {"decision": "stable", "reason": "pas assez de données récentes"}

    psi = {f: round(_psi(reference[f], recent[f].to_numpy(dtype=float)), 4) for f in FEATURES}
    decision = "drift" if max(psi.values()) > PSI_THRESHOLD else "stable"

    mlflow.set_experiment("fraud_drift_monitoring")
    with mlflow.start_run(run_name=f"drift_check_v{champion.version}"):
        mlflow.log_metrics({f"psi_{f}": v for f, v in psi.items()})
        mlflow.log_params({"champion_version": champion.version, "decision": decision,
                           "psi_threshold": PSI_THRESHOLD, "recent_window": len(recent)})
    return {"decision": decision, "psi": psi, "champion_version": champion.version}


# ---------------------------------------------------------------- entraînement + promotion

def _metrics(y_true, proba) -> dict:
    from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score
    preds = (proba >= 0.5).astype(int)
    return {
        "precision": precision_score(y_true, preds, zero_division=0),
        "recall": recall_score(y_true, preds, zero_division=0),
        "f1": f1_score(y_true, preds, zero_division=0),
        "roc_auc": roc_auc_score(y_true, proba) if len(set(y_true)) > 1 else 0.5,
    }


def train_candidate() -> dict:
    """Entraîne un candidat sur les données anciennes, l'évalue sur les 20 % les plus
    récentes (découpage temporel), évalue le champion sur ces MÊMES données, puis
    enregistre le candidat dans le registre (sans le promouvoir)."""
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    df = load_labeled_features()
    if len(df) < MIN_TRAINING_ROWS or df["is_fraud"].sum() < MIN_FRAUD_CASES:
        raise NotEnoughData(f"{len(df)} lignes étiquetées dont {int(df['is_fraud'].sum())} fraudes "
                            f"(minimum : {MIN_TRAINING_ROWS} lignes, {MIN_FRAUD_CASES} fraudes)")
    cut = int(len(df) * 0.8)
    train, holdout = df.iloc[:cut], df.iloc[cut:]
    if train["is_fraud"].nunique() < 2 or holdout["is_fraud"].sum() == 0:
        raise NotEnoughData("une des deux périodes ne contient aucune fraude")

    pipe = Pipeline([
        ("standard_scaler", StandardScaler()),
        ("classifier", RandomForestClassifier(n_estimators=100, class_weight="balanced", random_state=42)),
    ])
    pipe.fit(train[FEATURES], train["is_fraud"])
    candidate_metrics = _metrics(holdout["is_fraud"], pipe.predict_proba(holdout[FEATURES])[:, 1])

    mlflow = _mlflow()
    champion = get_champion()
    champion_f1 = None
    if champion is not None:
        champion_model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}@{CHAMPION_ALIAS}")
        champion_f1 = _metrics(holdout["is_fraud"],
                               champion_model.predict_proba(holdout[FEATURES])[:, 1])["f1"]

    mlflow.set_experiment("fraud_detection")
    with mlflow.start_run(run_name="candidate") as run:
        mlflow.log_params({"n_train": len(train), "n_holdout": len(holdout),
                           "train_until": str(train["computed_at"].max()),
                           "model": "RandomForest(class_weight=balanced)"})
        mlflow.log_metrics(candidate_metrics)
        if champion_f1 is not None:
            mlflow.log_metric("champion_f1_same_holdout", champion_f1)
        mlflow.log_dict(dict(zip(FEATURES, pipe.named_steps["classifier"].feature_importances_.tolist())),
                        "feature_importances.json")
        mlflow.log_dict(_reference_profile(train), "reference_profile.json")
        mlflow.sklearn.log_model(pipe, "model", registered_model_name=MODEL_NAME,
                                 input_example=train[FEATURES].head(3))
        run_id = run.info.run_id

    version = mlflow.MlflowClient().search_model_versions(f"run_id='{run_id}'")[0].version
    result = {"candidate_version": version, "candidate_f1": round(candidate_metrics["f1"], 4),
              "champion_version": champion.version if champion else None,
              "champion_f1": round(champion_f1, 4) if champion_f1 is not None else None}
    print(f"Candidat entraîné : {result}")
    return result


def promote_if_better(result: dict) -> dict:
    client = _mlflow().MlflowClient()
    champion_f1 = result["champion_f1"]
    promote = champion_f1 is None or result["candidate_f1"] >= champion_f1
    decision = "promu champion" if promote else "rejeté (champion conservé)"
    if promote:
        client.set_registered_model_alias(MODEL_NAME, CHAMPION_ALIAS, result["candidate_version"])
    client.set_model_version_tag(MODEL_NAME, result["candidate_version"], "promotion_decision", decision)
    print(f"Version {result['candidate_version']} : {decision} "
          f"(F1 candidat {result['candidate_f1']} / champion {champion_f1})")
    return {**result, "decision": decision}


# ---------------------------------------------------------------- scoring + write-back

def score_new_transactions() -> dict | None:
    """Score les transactions validées par dbt test et pas encore scorées.
    Retourne None s'il n'existe pas encore de modèle champion."""
    champion = get_champion()
    if champion is None:
        return None
    model = _mlflow().sklearn.load_model(f"models:/{MODEL_NAME}@{CHAMPION_ALIAS}")

    wh = psycopg2.connect(env("WH_ML_DSN"))
    try:
        with wh.cursor() as cur:
            cur.execute("""
                SELECT f.transaction_id
                FROM analytics.fact_transactions f
                LEFT JOIN ml.fraud_scores s ON s.transaction_id = f.transaction_id
                WHERE s.transaction_id IS NULL
                LIMIT 5000""")
            candidates = [r[0] for r in cur.fetchall()]

        scored = []
        if candidates:
            docs = list(_features_collection().find({"transaction_id": {"$in": candidates}}, {"_id": 0}))
            if docs:
                df = pd.DataFrame([{"transaction_id": d["transaction_id"], **d["features"]} for d in docs])
                proba = model.predict_proba(df[FEATURES])[:, 1]
                scored = [(int(t), round(float(p), 4), str(champion.version))
                          for t, p in zip(df["transaction_id"], proba)]
                with wh.cursor() as cur:
                    execute_values(cur, "INSERT INTO ml.fraud_scores (transaction_id, fraud_score, model_version) "
                                        "VALUES %s ON CONFLICT (transaction_id) DO NOTHING", scored)
                wh.commit()

        written_back = _write_back_to_oltp(wh)
    finally:
        wh.close()

    report = {
        "model_version": champion.version,
        "candidates": len(candidates),
        "scored": len(scored),
        "missing_features": len(candidates) - len(scored),   # événements pas encore ingérés : rattrapés au run suivant
        "high_risk": sum(1 for _, p, _ in scored if p >= HIGH_RISK),
        "written_back_to_oltp": written_back,
    }
    print(f"Scoring : {report}")
    return report


def _write_back_to_oltp(wh) -> int:
    """Réécrit dans l'OLTP tous les scores encore absents côté opérationnel.
    Idempotent et auto-réparant : un échec ici est rattrapé au passage suivant."""
    oltp = psycopg2.connect(env("OLTP_SCORING_DSN"))
    try:
        with oltp.cursor() as cur:
            cur.execute("SELECT transaction_id FROM oltp.transactions WHERE fraud_score IS NULL LIMIT 5000")
            missing = [r[0] for r in cur.fetchall()]
        if not missing:
            return 0
        with wh.cursor() as cur:
            cur.execute("SELECT transaction_id, fraud_score FROM ml.fraud_scores WHERE transaction_id = ANY(%s)",
                        (missing,))
            scores = cur.fetchall()
        if scores:
            with oltp.cursor() as cur:
                execute_values(cur,
                               "UPDATE oltp.transactions AS t SET fraud_score = data.score "
                               "FROM (VALUES %s) AS data(transaction_id, score) "
                               "WHERE t.transaction_id = data.transaction_id",
                               scores)
            oltp.commit()
        return len(scores)
    finally:
        oltp.close()
