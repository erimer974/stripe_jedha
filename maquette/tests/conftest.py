"""Tests automatisés de la gestion des erreurs (Livrable 5 §11).
Lancement : docker compose exec airflow python -m pytest -q /opt/airflow/tests -p no:cacheprovider"""
import os
import sys

import pytest

# Aucun appel au vrai registre MLflow pendant les tests : le service de scoring reste sans modèle
os.environ["MLFLOW_TRACKING_URI"] = "http://127.0.0.1:9"

sys.path.insert(0, "/opt/airflow")
sys.path.insert(0, "/opt/airflow/sources")


@pytest.fixture(autouse=True)
def no_real_alerts(monkeypatch):
    """Les tests ne doivent pas écrire de vraies alertes dans l'entrepôt."""
    monkeypatch.delenv("ALERTS_DSN", raising=False)
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)


class FakeMessage:
    """Message Kafka minimal pour les tests."""

    def __init__(self, topic: str, value: bytes, partition: int = 0, offset: int = 0):
        self._topic, self._value, self._partition, self._offset = topic, value, partition, offset

    def topic(self):
        return self._topic

    def value(self):
        return self._value

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset
