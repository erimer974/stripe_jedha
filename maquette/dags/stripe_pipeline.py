"""
============================================================
DAG principal — bande "Apache Airflow (orchestration)" du schéma
------------------------------------------------------------
Ordre identique au plan cible (05_pipeline/00_plan_pipeline_cible.svg) et
au document technique 05_pipeline/README.md :

    EL (rôle d'Airbyte)  ->  dbt run  ->  dbt test (qualité)  ->  scoring fraude

Planifié en micro-batch toutes les 5 minutes (≈ temps réel, sans CDC).
Le scoring ne s'exécute que si les tests qualité passent : aucune donnée
non validée n'atteint le modèle.
============================================================
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

DBT_DIR = "/opt/airflow/dbt_stripe"


def _sync_oltp():
    from pipeline import el_sync
    return el_sync.sync_oltp()


def _sync_reference():
    from pipeline import el_sync
    return el_sync.sync_reference()


def _sync_events():
    from pipeline import el_sync
    return el_sync.sync_events()


def _fraud_scoring():
    from pipeline import ml_fraud
    report = ml_fraud.score_new_transactions()
    if report is None:
        raise AirflowSkipException(
            "Aucun modèle 'champion' dans le registre MLflow : lancer le DAG stripe_model_retraining.")
    return report


with DAG(
    dag_id="stripe_pipeline",
    description="EL -> dbt run -> dbt test -> scoring fraude (micro-batch 5 min)",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,          # un seul run à la fois : les curseurs incrémentaux restent cohérents
    default_args={"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["bc02", "stripe", "maquette", "pipeline"],
    doc_md=__doc__,
) as dag:

    with TaskGroup("el_sync", tooltip="Ingestion EL — rôle d'Airbyte") as el_sync:
        PythonOperator(task_id="oltp_postgres_to_raw", python_callable=_sync_oltp)
        PythonOperator(task_id="reference_files_to_raw", python_callable=_sync_reference)
        PythonOperator(task_id="app_events_to_mongodb", python_callable=_sync_events)

    dbt_transform = BashOperator(
        task_id="dbt_transform",
        bash_command=f"cd {DBT_DIR} && dbt run",
    )

    dbt_test_quality = BashOperator(
        task_id="dbt_test_quality",
        bash_command=f"cd {DBT_DIR} && dbt source freshness && dbt test",
    )

    fraud_scoring = PythonOperator(task_id="fraud_scoring", python_callable=_fraud_scoring)

    el_sync >> dbt_transform >> dbt_test_quality >> fraud_scoring
