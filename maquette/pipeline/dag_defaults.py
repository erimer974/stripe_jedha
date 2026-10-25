"""
Règles communes aux tâches Airflow (Livrable 5 §11) :
  - nouvel essai automatique, avec une attente qui double à chaque échec ;
  - délai maximal par tâche : une tâche bloquée est arrêtée au lieu d'attendre indéfiniment ;
  - après la dernière tentative, ALERTE (pipeline/alerting.py).
"""
from __future__ import annotations

from datetime import timedelta

from pipeline.alerting import on_task_failure


def default_args(owner: str, retries: int = 2, timeout_minutes: int = 15) -> dict:
    return {
        "owner": owner,
        "retries": retries,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,        # 1 min, 2 min, 4 min...
        "max_retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(minutes=timeout_minutes),
        "on_failure_callback": on_task_failure,
    }
