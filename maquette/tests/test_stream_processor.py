"""Voie flux : un message invalide est isolé (quarantaine), une indisponibilité est réessayée."""
import json
import os
from decimal import Decimal

import psycopg2
import pytest
from pymongo.errors import AutoReconnect

from conftest import FakeMessage
from pipeline import stream_processor as sp
from pipeline.errors import InvalidRecord, is_transient

VALID_TX = {"transaction_id": 7, "customer_id": 1, "merchant_id": 2, "payment_method_id": 1,
            "currency_code": "EUR", "amount": "12.50", "ip_geolocation": None, "device_type": "mobile",
            "status": "succeeded", "created_at": "2026-10-16T10:00:00Z", "__op": "c", "__source_lsn": 42}


def as_bytes(record) -> bytes:
    return json.dumps(record).encode()


# ---------------------------------------------------------------- messages de la capture des changements

def test_cdc_valid_transaction_is_typed():
    table, key, row, lsn = sp.parse_cdc("cdc.oltp.transactions", as_bytes(VALID_TX))
    assert (table, key, lsn) == ("transactions", 7, 42)
    assert row[5] == Decimal("12.50")


def test_cdc_delete_is_ignored():
    assert sp.parse_cdc("cdc.oltp.transactions", as_bytes({**VALID_TX, "__op": "d"})) is None


@pytest.mark.parametrize("value, reason", [
    (b"{pas du json", "JSON illisible"),
    (as_bytes([1, 2]), "pas un objet"),
    (as_bytes({**VALID_TX, "amount": "abc"}), "amount"),
    (as_bytes({**VALID_TX, "transaction_id": None}), "transaction_id"),
    (as_bytes({**VALID_TX, "customer_id": "douze"}), "customer_id"),
    (as_bytes({**VALID_TX, "created_at": "hier"}), "created_at"),
])
def test_cdc_invalid_messages_are_rejected_with_a_reason(value, reason):
    with pytest.raises(InvalidRecord, match=reason):
        sp.parse_cdc("cdc.oltp.transactions", value)


def test_cdc_unknown_table_is_rejected():
    with pytest.raises(InvalidRecord, match="table inconnue"):
        sp.parse_cdc("cdc.oltp.cartes_bancaires", as_bytes(VALID_TX))


# ---------------------------------------------------------------- événements applicatifs et scores

def test_feature_event_must_respect_the_mongodb_contract():
    event = {"event_id": "e1", "transaction_id": 7, "features": {"amt": 10, "geo_distance_km": 1.5},
             "computed_at": "2026-10-16T10:00:00+00:00"}
    with pytest.raises(InvalidRecord, match="numériques"):
        sp.parse_event("events.risk_features", as_bytes(event))


def test_session_event_gets_its_id():
    collection, key, doc = sp.parse_event("events.sessions", as_bytes(
        {"event_id": "s1", "customer_id": 3, "started_at": "2026-10-16T10:00:00+00:00", "events": []}))
    assert (collection, key, doc["_id"]) == ("user_sessions", {"_id": "s1"}, "s1")


def test_event_without_id_is_rejected():
    with pytest.raises(InvalidRecord, match="event_id"):
        sp.parse_event("logs.app", as_bytes({"level": "ERROR"}))


@pytest.mark.parametrize("score", [1.5, -0.1, "élevé", None])
def test_score_outside_zero_one_is_rejected(score):
    with pytest.raises(InvalidRecord, match="fraud_score"):
        sp.parse_score(as_bytes({"transaction_id": 7, "fraud_score": score, "model_version": "1"}))


def test_split_keeps_the_good_messages_and_isolates_the_bad_one():
    batch = [FakeMessage("fraud.scores", as_bytes({"transaction_id": 1, "fraud_score": 0.2, "model_version": "1"})),
             FakeMessage("fraud.scores", b"\xff\xfe", offset=1),
             FakeMessage("fraud.scores", as_bytes({"transaction_id": 2, "fraud_score": 0.9, "model_version": "1"}))]
    valid, rejected = sp._split(batch, lambda m: sp.parse_score(m.value()))
    assert [row[0] for _, row in valid] == [1, 2]
    assert len(rejected) == 1 and "JSON illisible" in rejected[0][1]


# ---------------------------------------------------------------- erreurs passagères ou définitives

def test_unavailability_is_transient_but_bad_data_is_not():
    assert is_transient(psycopg2.OperationalError("connexion refusée"))
    assert is_transient(AutoReconnect("primaire en cours d'élection"))
    assert not is_transient(psycopg2.DataError("invalid input syntax for type numeric"))
    assert not is_transient(InvalidRecord("JSON illisible"))


@pytest.mark.skipif(not os.environ.get("WH_LOADER_DSN"), reason="entrepôt non disponible")
def test_database_refusal_isolates_only_the_faulty_rows():
    """Intégration : la base refuse 2 lignes sur 3 ; le lot n'est pas perdu, seules les fautives
    sont renvoyées (pour la quarantaine), la bonne est écrite."""
    conn = psycopg2.connect(os.environ["WH_LOADER_DSN"])
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE essai (id INT PRIMARY KEY, amount NUMERIC CHECK (amount > 0))")
        items = [("bon", (1, "10.5")), ("négatif", (2, "-3")), ("pas un nombre", (3, "abc"))]
        refused = sp._write_rows(conn, "INSERT INTO essai (id, amount) VALUES %s", items)
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM essai")
            written = [r[0] for r in cur.fetchall()]
        assert written == [1]
        assert [msg for msg, _ in refused] == ["négatif", "pas un nombre"]
        assert all(reason.startswith("refusé par la base") for _, reason in refused)
    finally:
        conn.rollback()
        conn.close()
