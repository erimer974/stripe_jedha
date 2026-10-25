"""Référentiels, disjoncteur, service de scoring, capacité et alertes : comportements en cas d'erreur."""
from datetime import date

import pytest

from pipeline import capacity, el_sync
from pipeline.alerting import raise_alert


# ---------------------------------------------------------------- référentiels

def test_reference_file_rows_are_validated_one_by_one():
    stream = el_sync.REFERENCE_FILES[1]                     # exchange_rates
    records = [
        {"currency_code": "EUR", "rate_date": "2026-01-01", "rate_to_usd": "1.08"},
        {"currency_code": "eur", "rate_date": "2026-01-01", "rate_to_usd": "1.08"},
        {"currency_code": "GBP", "rate_date": "01/01/2026", "rate_to_usd": "1.27"},
        {"currency_code": "JPY", "rate_date": "2026-01-01", "rate_to_usd": "-1"},
    ]
    valid, rejected = el_sync.validate_file(stream, records)
    assert valid == [("EUR", date(2026, 1, 1), 1.08)]
    assert [line for line, _, _ in rejected] == [3, 4, 5]     # numéros de ligne du fichier
    assert "positif" in rejected[2][2]


# ---------------------------------------------------------------- disjoncteur de l'API de paiement

def test_circuit_breaker_opens_after_three_failures_then_tries_again(monkeypatch):
    import app_simulator
    now = [1000.0]
    monkeypatch.setattr(app_simulator.time, "monotonic", lambda: now[0])
    breaker = app_simulator.CircuitBreaker()
    for _ in range(3):
        assert breaker.allows_call()
        breaker.record(success=False)
    assert not breaker.allows_call()                         # ouvert : règle de repli, sans attendre
    now[0] += breaker.OPEN_SECONDS
    assert breaker.allows_call()                             # 30 s plus tard : un appel d'essai
    breaker.record(success=True)
    assert breaker.allows_call() and breaker.opened_at is None


def test_fallback_rule_challenges_large_or_fast_payments():
    import app_simulator
    assert app_simulator._fallback_decision({"amt": 5000, "velocity_1h": 0}) == "challenge"
    assert app_simulator._fallback_decision({"amt": 20, "velocity_1h": 9}) == "challenge"
    assert app_simulator._fallback_decision({"amt": 20, "velocity_1h": 1}) == "accept"


# ---------------------------------------------------------------- service de scoring

@pytest.fixture()
def scoring(monkeypatch):
    from pipeline import scoring_api
    monkeypatch.setitem(scoring_api._state, "model", None)
    monkeypatch.setitem(scoring_api._state, "version", None)
    return scoring_api


def test_scoring_rejects_incomplete_requests(scoring):
    response = scoring.app.test_client().post("/score", json={"amt": 10})
    assert response.status_code == 400
    assert "features manquantes" in response.get_json()["error"]


def test_scoring_answers_503_without_a_model(scoring):
    response = scoring.app.test_client().post("/score", json={"amt": 10, "geo_distance_km": 1, "velocity_1h": 0})
    assert response.status_code == 503


def test_scoring_challenges_high_scores(scoring, monkeypatch):
    import numpy as np

    class AlwaysFraud:
        def predict_proba(self, features):
            return np.array([[0.05, 0.95]])
    monkeypatch.setitem(scoring._state, "model", AlwaysFraud())
    monkeypatch.setitem(scoring._state, "version", "9")
    body = scoring.app.test_client().post(
        "/score", json={"amt": 4000, "geo_distance_km": 3000, "velocity_1h": 15}).get_json()
    assert (body["decision"], body["model_version"]) == ("challenge", "9")


# ---------------------------------------------------------------- capacité et alertes

@pytest.mark.parametrize("utilization, status", [(10, "OK"), (50, "VIGILANCE"), (70, "VIGILANCE"), (70.1, "AGRANDIR")])
def test_capacity_status_thresholds(utilization, status):
    assert capacity._status(utilization) == status


def test_an_alert_never_breaks_the_caller(monkeypatch, capsys):
    monkeypatch.setenv("ALERTS_DSN", "postgresql://personne@hote-inexistant:5432/x?connect_timeout=1")
    raise_alert("test", "CRITICAL", "entrepôt injoignable")      # ne doit pas lever d'exception
    output = capsys.readouterr().out
    assert "ALERTE CRITICAL" in output and "non historisée" in output


def test_scoring_answers_400_for_non_numeric_features(scoring, monkeypatch):
    monkeypatch.setitem(scoring._state, "model", object())
    response = scoring.app.test_client().post("/score", json={"amt": "beaucoup", "geo_distance_km": 1, "velocity_1h": 0})
    assert response.status_code == 400


def test_a_broken_model_returns_500_instead_of_crashing(scoring, monkeypatch):
    class Broken:
        def predict_proba(self, features):
            raise ValueError("modèle corrompu")
    monkeypatch.setitem(scoring._state, "model", Broken())
    monkeypatch.setitem(scoring._state, "version", "3")
    response = scoring.app.test_client().post("/score", json={"amt": 10, "geo_distance_km": 1, "velocity_1h": 0})
    assert response.status_code == 500 and "modèle 3" in response.get_json()["error"]
