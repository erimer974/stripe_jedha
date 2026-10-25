"""
============================================================
DAG principal — VOIE LOTS (bande "Apache Airflow (orchestration)" du schéma)
------------------------------------------------------------
Les transactions arrivent déjà en continu dans la couche raw par la voie flux
(Debezium -> Kafka -> stream_processor). Toutes les 5 minutes, ce DAG :

    référentiels (rôle d'Airbyte)  ->  dbt run  ->  dbt test (qualité)  ->  scoring de rattrapage

Le scoring de rattrapage ne traite que les transactions restées SANS score
(service de scoring indisponible au moment du paiement) et seulement si les
tests qualité passent : aucune donnée non validée n'atteint le modèle.
============================================================
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from pipeline.dag_defaults import default_args

DBT_DIR = "/opt/airflow/dbt_stripe"


def _sync_reference():
    from pipeline import el_sync
    return el_sync.sync_reference()


def _fraud_scoring_catch_up():
    from pipeline import ml_fraud
    report = ml_fraud.score_new_transactions()
    if report is None:
        raise AirflowSkipException(
            "Aucun modèle 'champion' dans le registre MLflow : lancer le DAG stripe_model_retraining.")
    return report


with DAG(
    dag_id="stripe_pipeline",
    description="référentiels -> dbt run -> dbt test -> scoring de rattrapage (toutes les 5 min)",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,          # un seul run à la fois : dbt et le rattrapage ne se chevauchent pas
    default_args=default_args("data-eng", retries=2, timeout_minutes=10),
    tags=["bc02", "stripe", "maquette", "pipeline"],
    doc_md=__doc__,
) as dag:

    with TaskGroup("el_sync", tooltip="Référentiels par lots — rôle d'Airbyte") as el_sync:
        PythonOperator(task_id="reference_files_to_raw", python_callable=_sync_reference)

    dbt_transform = BashOperator(
        task_id="dbt_transform",
        bash_command=f"cd {DBT_DIR} && dbt run",
    )

    dbt_test_quality = BashOperator(
        task_id="dbt_test_quality",
        bash_command=f"cd {DBT_DIR} && dbt source freshness && dbt test",
    )

    fraud_scoring = PythonOperator(task_id="fraud_scoring_catch_up", python_callable=_fraud_scoring_catch_up)

    el_sync >> dbt_transform >> dbt_test_quality >> fraud_scoring
