"""
============================================================
DAG d'entraînement continu — boucle ML du schéma
------------------------------------------------------------
    check_drift ──> train_candidate ──> promote_if_better
               └──> no_retraining_needed

check_drift : aucun champion (démarrage) ou dérive des features (PSI > 0,2,
rôle d'Evidently) -> réentraînement ; sinon rien.
Le candidat n'est promu "champion" que s'il est au moins aussi bon que le
champion en place, évalués tous deux sur les mêmes données récentes.
============================================================
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from pipeline.dag_defaults import default_args


def _check_drift(ti):
    from pipeline import ml_fraud
    result = ml_fraud.check_drift()
    print(f"Contrôle de dérive : {result}")
    ti.xcom_push(key="drift", value=result)
    return "train_candidate" if result["decision"] in ("bootstrap", "drift") else "no_retraining_needed"


def _train_candidate():
    from pipeline import ml_fraud
    try:
        return ml_fraud.train_candidate()
    except ml_fraud.NotEnoughData as exc:
        raise AirflowSkipException(f"Entraînement reporté : {exc}")


def _promote_if_better(ti):
    from pipeline import ml_fraud
    return ml_fraud.promote_if_better(ti.xcom_pull(task_ids="train_candidate"))


with DAG(
    dag_id="stripe_model_retraining",
    description="Dérive -> entraînement d'un candidat -> promotion si meilleur",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args("ml-eng", retries=1, timeout_minutes=30),
    tags=["bc02", "stripe", "maquette", "ml"],
    doc_md=__doc__,
) as dag:

    check_drift = BranchPythonOperator(task_id="check_drift", python_callable=_check_drift)
    no_retraining_needed = EmptyOperator(task_id="no_retraining_needed")
    train_candidate = PythonOperator(task_id="train_candidate", python_callable=_train_candidate)
    promote_if_better = PythonOperator(task_id="promote_if_better", python_callable=_promote_if_better)

    check_drift >> [train_candidate, no_retraining_needed]
    train_candidate >> promote_if_better
