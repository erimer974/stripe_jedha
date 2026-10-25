"""
============================================================
DAG de conformité quotidien — bande "Sécurité & conformité" du schéma
------------------------------------------------------------
    run_compliance_checks  ->  assert_blocking_controls

Contrôles RGPD, PCI-DSS, audit et moindre privilège sur l'OLTP et
l'entrepôt, historisés dans warehouse.audit.compliance_reports.
Un contrôle bloquant en échec fait échouer le DAG (alerte).
============================================================
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.dag_defaults import default_args


def _run_checks(run_id):
    from pipeline import compliance
    return compliance.run_compliance_checks(run_id)


def _assert_blocking(run_id):
    from pipeline import compliance
    compliance.assert_blocking_controls(run_id)


with DAG(
    dag_id="stripe_compliance_daily",
    description="Rapport de conformité automatisé (RGPD, PCI-DSS, audit, RBAC)",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args("data-governance", retries=1, timeout_minutes=10),
    tags=["bc02", "stripe", "maquette", "conformite"],
    doc_md=__doc__,
) as dag:

    run_compliance_checks = PythonOperator(
        task_id="run_compliance_checks", python_callable=_run_checks,
        op_kwargs={"run_id": "{{ run_id }}"},
    )
    assert_blocking_controls = PythonOperator(
        task_id="assert_blocking_controls", python_callable=_assert_blocking,
        op_kwargs={"run_id": "{{ run_id }}"},
    )

    run_compliance_checks >> assert_blocking_controls
