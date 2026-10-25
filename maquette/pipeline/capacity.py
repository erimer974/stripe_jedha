"""
============================================================
MONTÉE EN CHARGE — surveillance de capacité (Livrable 5 §8.3)
------------------------------------------------------------
Chaque heure :
  1. crée les partitions mensuelles des 3 mois à venir (rôle capacity_monitor) ;
  2. mesure le débit réel de l'OLTP : pic (meilleure minute de la dernière heure)
     et moyenne, en transactions par seconde ;
  3. le compare à la capacité de conception (CAPACITY_TPS) :
        < 50 %  OK          fonctionnement normal
        50-70 % VIGILANCE   on mesure et on prépare
        > 70 %  AGRANDIR    on ajoute des serveurs (livrable 0 §4.2)
  4. mesure le retard (lag) des consommateurs Kafka : s'il grandit, on ajoute
     des consommateurs, jusqu'au nombre de partitions ;
  5. historise le tout dans warehouse.audit.capacity_reports.

Le statut AGRANDIR fait échouer la tâche : l'échec est l'alerte.
============================================================
"""
from __future__ import annotations

import os

import psycopg2
from confluent_kafka import Consumer, TopicPartition

from pipeline.config import env

CONSUMER_GROUPS = {
    "sink-raw": ["cdc.oltp.transactions", "cdc.oltp.customers", "cdc.oltp.merchants", "cdc.oltp.payment_methods"],
    "sink-mongodb": ["events.risk_features", "events.sessions", "events.feedback", "logs.app"],
    "sink-ml-scores": ["fraud.scores"],
}
VIGILANCE_PCT = 50
SCALE_OUT_PCT = 70


class ScaleOutNeeded(Exception):
    """Levée quand le débit dépasse 70 % de la capacité : il faut agrandir."""


def _status(utilization_pct: float) -> str:
    if utilization_pct > SCALE_OUT_PCT:
        return "AGRANDIR"
    if utilization_pct >= VIGILANCE_PCT:
        return "VIGILANCE"
    return "OK"


def _kafka_lag() -> dict[str, int]:
    """Retard par groupe : messages publiés mais pas encore écrits par le consommateur."""
    lags = {}
    for group, topics in CONSUMER_GROUPS.items():
        consumer = Consumer({"bootstrap.servers": env("KAFKA_BOOTSTRAP_SERVERS"), "group.id": group,
                             "enable.auto.commit": False})
        try:
            partitions = []
            for topic in topics:
                metadata = consumer.list_topics(topic, timeout=10).topics[topic]
                partitions += [TopicPartition(topic, p) for p in metadata.partitions]
            lag = 0
            for tp in consumer.committed(partitions, timeout=10):
                _, high = consumer.get_watermark_offsets(tp, timeout=10)
                lag += high - (tp.offset if tp.offset >= 0 else 0)
            lags[group] = lag
        finally:
            consumer.close()
    return lags


def check_capacity() -> dict:
    capacity_tps = float(os.environ.get("CAPACITY_TPS", "290"))

    with psycopg2.connect(env("OLTP_CAPACITY_DSN")) as oltp, oltp.cursor() as cur:
        cur.execute("SELECT oltp.ensure_monthly_partitions(3)")
        created = cur.fetchone()[0]
        cur.execute("""
            SELECT coalesce(max(per_minute), 0) / 60.0, coalesce(sum(per_minute), 0) / 3600.0
            FROM (SELECT count(*) AS per_minute
                  FROM oltp.transactions
                  WHERE created_at > now() - interval '1 hour'
                  GROUP BY date_trunc('minute', created_at)) m""")
        peak_tps, avg_tps = (float(v) for v in cur.fetchone())
        cur.execute("""
            SELECT count(*) FILTER (WHERE c.relname > 'transactions_' || to_char(now(), 'YYYY_MM')
                                         AND c.relname <> 'transactions_default'),
                   sum(pg_total_relation_size(c.oid)) / 1024.0 / 1024.0
            FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
            WHERE i.inhparent = 'oltp.transactions'::regclass""")
        partitions_ahead, oltp_size_mb = cur.fetchone()
    default_rows = _default_partition_rows()

    lags = _kafka_lag()
    utilization = round(100 * peak_tps / capacity_tps, 1)
    status = _status(utilization)
    report = {"peak_tps": round(peak_tps, 2), "avg_tps": round(avg_tps, 2), "capacity_tps": capacity_tps,
              "utilization_pct": utilization, "status": status, "kafka_lag": lags,
              "oltp_size_mb": round(float(oltp_size_mb or 0), 1), "partitions_created_now": created,
              "partitions_ahead": partitions_ahead, "default_partition_rows": default_rows}

    with psycopg2.connect(env("WH_AUDIT_DSN")) as wh, wh.cursor() as cur:
        cur.execute(
            """INSERT INTO audit.capacity_reports (peak_tps, avg_tps, capacity_tps, utilization_pct, status,
                   kafka_max_lag, oltp_size_mb, partitions_ahead, default_partition_rows)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (report["peak_tps"], report["avg_tps"], capacity_tps, utilization, status,
             max(lags.values()) if lags else None, report["oltp_size_mb"], partitions_ahead, default_rows))

    print(f"Capacité : {report}")
    if status == "AGRANDIR":
        raise ScaleOutNeeded(f"débit de pointe {peak_tps:.1f} tx/s = {utilization} % de la capacité "
                             f"({capacity_tps:.0f} tx/s) : au-delà de {SCALE_OUT_PCT} %, il faut agrandir")
    return report


def _default_partition_rows() -> int:
    """La partition par défaut doit rester vide : sinon une partition mensuelle manque.
    Le rôle de surveillance n'a pas le droit de lire les données : il lit l'estimation du catalogue."""
    with psycopg2.connect(env("OLTP_CAPACITY_DSN")) as oltp, oltp.cursor() as cur:
        cur.execute("""SELECT coalesce(n_live_tup, 0) FROM pg_stat_user_tables
                       WHERE schemaname = 'oltp' AND relname = 'transactions_default'""")
        row = cur.fetchone()
    return int(row[0]) if row else 0
