"""
============================================================
SÉCURITÉ & CONFORMITÉ — rapport automatisé quotidien (Livrable 6)
------------------------------------------------------------
Exécute les contrôles sur l'OLTP et l'entrepôt avec le rôle "auditor"
(lecture seule des vues d'audit), historise le résultat dans
warehouse.audit.compliance_reports, puis fait échouer le DAG si un
contrôle bloquant est en échec : l'échec est l'alerte.
============================================================
"""
from __future__ import annotations

import psycopg2

from pipeline.config import env


def _rows(cur, sql: str) -> list[tuple]:
    cur.execute(sql)
    return cur.fetchall()


def _oltp_checks(cur) -> list[tuple]:
    checks = []

    violations = _rows(cur, "SELECT role_name FROM audit.pii_access_violations")
    checks.append(("rgpd_pii_access", True, "FAIL" if violations else "PASS",
                   f"rôles lisant l'email en clair : {[v[0] for v in violations] or 'aucun'}"))

    card_columns = _rows(cur, "SELECT table_name || '.' || column_name FROM audit.pci_card_data_columns")
    checks.append(("pci_no_card_data", True, "FAIL" if card_columns else "PASS",
                   f"colonnes de données de carte : {[c[0] for c in card_columns] or 'aucune'}"))

    settings = dict(_rows(cur, "SELECT name, setting FROM audit.security_settings"))
    audit_ok = (settings.get("log_connections") == "on"
                and settings.get("log_disconnections") == "on"
                and settings.get("log_statement") in ("ddl", "mod", "all"))
    checks.append(("audit_logging_enabled", True, "PASS" if audit_ok else "FAIL",
                   f"log_connections={settings.get('log_connections')}, "
                   f"log_disconnections={settings.get('log_disconnections')}, "
                   f"log_statement={settings.get('log_statement')}"))

    # Non bloquant dans la maquette : TLS fait partie de la politique cible, non reproduite ici.
    checks.append(("tls_in_transit", False, "PASS" if settings.get("ssl") == "on" else "FAIL",
                   f"ssl={settings.get('ssl')} (cible : on, certificat monté)"))

    erased = _rows(cur, "SELECT erased_customers FROM audit.erasure_report")[0][0]
    checks.append(("rgpd_erasure_requests", False, "INFO", f"clients anonymisés : {erased}"))

    grants = _rows(cur, "SELECT grantee, count(*) FROM audit.grants_report GROUP BY grantee ORDER BY grantee")
    checks.append(("access_review", False, "INFO", "droits par rôle : " + ", ".join(f"{g}={n}" for g, n in grants)))
    return checks


def _warehouse_checks(cur) -> list[tuple]:
    checks = []

    sensitive = _rows(cur, "SELECT schema_name || '.' || table_name || '.' || column_name "
                           "FROM audit.sensitive_columns_in_warehouse")
    checks.append(("rgpd_pci_no_sensitive_columns", True, "FAIL" if sensitive else "PASS",
                   f"colonnes sensibles dans l'entrepôt : {[s[0] for s in sensitive] or 'aucune'}"))

    forbidden = _rows(cur, "SELECT schema_name FROM audit.analyst_forbidden_access")
    checks.append(("least_privilege_analyst", True, "FAIL" if forbidden else "PASS",
                   f"schémas interdits accessibles à data_analyst : {[f[0] for f in forbidden] or 'aucun'}"))

    stale = _rows(cur, "SELECT stream, minutes_since_sync FROM audit.ingestion_freshness "
                       "WHERE minutes_since_sync > 30")
    checks.append(("ingestion_freshness", False, "FAIL" if stale else "PASS",
                   f"flux non synchronisés depuis plus de 30 min : {stale or 'aucun'}"))
    return checks


def run_compliance_checks(run_id: str) -> dict:
    results = []
    with psycopg2.connect(env("OLTP_AUDIT_DSN")) as oltp, oltp.cursor() as cur:
        results += [("oltp", *check) for check in _oltp_checks(cur)]

    wh = psycopg2.connect(env("WH_AUDIT_DSN"))
    try:
        with wh.cursor() as cur:
            results += [("warehouse", *check) for check in _warehouse_checks(cur)]
            for system, name, blocking, status, detail in results:
                cur.execute(
                    "INSERT INTO audit.compliance_reports (run_id, system, check_name, blocking, status, detail) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (run_id, system, name, blocking, status, detail),
                )
        wh.commit()
    finally:
        wh.close()

    for system, name, blocking, status, detail in results:
        print(f"[{status:4}] {'BLOQUANT' if blocking else 'info    '} {system}.{name} — {detail}")
    return {"checks": len(results),
            "blocking_failures": sum(1 for r in results if r[2] and r[3] == "FAIL")}


def assert_blocking_controls(run_id: str) -> None:
    wh = psycopg2.connect(env("WH_AUDIT_DSN"))
    try:
        with wh.cursor() as cur:
            cur.execute("SELECT system || '.' || check_name, detail FROM audit.compliance_reports "
                        "WHERE run_id = %s AND blocking AND status = 'FAIL'", (run_id,))
            failures = cur.fetchall()
    finally:
        wh.close()
    if failures:
        raise RuntimeError(f"Contrôles de conformité bloquants en échec : {failures}")
    print("Tous les contrôles bloquants sont conformes.")
