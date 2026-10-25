"""
============================================================
DAG de surveillance de capacité — « mesurer avant d'agrandir »
------------------------------------------------------------
Chaque heure : partitions des mois à venir, débit de pointe comparé à la
capacité de conception, retard des consommateurs Kafka, historique dans
audit.capacity_reports. Au-delà de 70 % de la capacité, la tâche échoue :
c'est le signal pour ajouter des serveurs (livrable 0 §4.2, livrable 5 §8.3).
============================================================
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.dag_defaults import default_args


def _check_capacity():
    from pipeline import capacity
    return capacity.check_capacity()


with DAG(
    dag_id="stripe_capacity",
    description="partitions à venir + débit vs capacité + retard Kafka (toutes les heures)",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    default_args=default_args("data-eng", retries=0, timeout_minutes=5),   # un dépassement n'est pas une panne passagère
    tags=["bc02", "stripe", "maquette", "capacite"],
    doc_md=__doc__,
) as dag:
    PythonOperator(task_id="check_capacity", python_callable=_check_capacity)
