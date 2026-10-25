"""
============================================================
ALERTES — prévenir un humain (Livrable 5 §11)
------------------------------------------------------------
Chaque alerte est :
  1. écrite dans le journal du service (toujours) ;
  2. historisée dans warehouse.audit.alerts (rôle « alerter », INSERT seul) ;
  3. envoyée à l'outil d'astreinte si ALERT_WEBHOOK_URL est défini
     (cible : Slack, PagerDuty ; non configuré dans la maquette).
Une alerte qui ne peut pas être envoyée ne fait jamais échouer le traitement.
============================================================
"""
from __future__ import annotations

import json
import os
import urllib.request
from datetime import datetime

import psycopg2

SEVERITIES = ("INFO", "WARNING", "CRITICAL")


def raise_alert(source: str, severity: str, message: str) -> None:
    severity = severity if severity in SEVERITIES else "WARNING"
    print(f"[ALERTE {severity} {datetime.now():%H:%M:%S}] {source} : {message}", flush=True)

    dsn = os.environ.get("ALERTS_DSN")
    if dsn:
        try:
            with psycopg2.connect(dsn, connect_timeout=3) as conn, conn.cursor() as cur:
                cur.execute("INSERT INTO audit.alerts (severity, source, message) VALUES (%s, %s, %s)",
                            (severity, source, message[:2000]))
            conn.close()
        except psycopg2.Error as exc:
            print(f"[alerte non historisée : entrepôt indisponible] {exc}", flush=True)

    webhook = os.environ.get("ALERT_WEBHOOK_URL")
    if webhook:
        try:
            body = json.dumps({"text": f"[{severity}] {source} : {message}"}).encode()
            request = urllib.request.Request(webhook, data=body, headers={"Content-Type": "application/json"})
            urllib.request.urlopen(request, timeout=5).close()
        except OSError as exc:
            print(f"[alerte non envoyée à l'astreinte] {exc}", flush=True)


def on_task_failure(context) -> None:
    """Rappel Airflow : appelé quand une tâche échoue après toutes ses tentatives."""
    ti = context["task_instance"]
    error = context.get("exception")
    raise_alert(source=f"airflow:{ti.dag_id}.{ti.task_id}", severity="CRITICAL",
                message=f"échec définitif, toutes les tentatives épuisées : {error}")
