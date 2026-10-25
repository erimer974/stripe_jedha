#!/usr/bin/env bash
# ============================================================
# GESTION DES ERREURS — injection de données invalides (Livrable 5 §11)
# ------------------------------------------------------------
# 1. On publie dans Kafka trois messages invalides (JSON cassé, montant
#    « abc », features incomplètes) au milieu du trafic normal.
#    -> ils partent en QUARANTAINE (raw._quarantine) avec leur raison ;
#       le flux continue, une ALERTE est enregistrée.
# 2. On publie une transaction bien formée mais dont le client n'existe pas.
#    -> elle entre dans RAW, mais dbt la met en quarantaine métier
#       (analytics.quarantine_transactions, raison « client inconnu »)
#       au lieu de la perdre en silence dans une jointure.
#
# Usage, depuis le dossier maquette (Git Bash) : bash queries/08_injection_erreurs.sh
# ============================================================
set -euo pipefail
source .env
wh() { docker compose exec -T warehouse psql -U "$PG_ADMIN_USER" -d warehouse -c "$1"; }
produce() { printf '%s\n' "$3" | docker compose exec -T redpanda rpk topic produce "$1" -k "$2" > /dev/null; }

echo "=== 1. Messages invalides publiés dans Kafka"
produce fraud.scores 990000001 '{pas du json'
produce cdc.oltp.transactions '{"transaction_id":990000002}' \
  '{"transaction_id":990000002,"customer_id":1,"merchant_id":1,"payment_method_id":1,"currency_code":"EUR","amount":"abc","ip_geolocation":null,"device_type":"mobile","status":"succeeded","created_at":"2026-10-16T10:00:00Z","__op":"c"}'
produce events.risk_features 990000003 \
  '{"event_id":"e-test","transaction_id":990000003,"features":{"amt":10},"computed_at":"2026-10-16T10:00:00+00:00"}'

echo "=== 2. Transaction bien formée, client inconnu"
produce cdc.oltp.transactions '{"transaction_id":990000004}' \
  '{"transaction_id":990000004,"customer_id":999999,"merchant_id":1,"payment_method_id":1,"currency_code":"EUR","amount":"42.00","ip_geolocation":null,"device_type":"mobile","status":"succeeded","created_at":"2026-10-16T10:00:00Z","__op":"c"}'

sleep 10
echo "=== 3. Le consommateur a continué, les rejets sont en quarantaine"
docker compose logs --since 20s stream-processor | grep -E "quarantaine|ALERTE" | sed 's/^stream-processor-1  | /  /' || true
wh "SELECT quarantine_id, source, location, error FROM raw._quarantine ORDER BY quarantine_id DESC LIMIT 3"

echo "=== 4. Quarantaine métier (dbt) : la transaction au client inconnu"
# +fact_transactions : la table de faits ET ce dont elle dépend (staging, dimensions) ; sans les
# dimensions, les transactions des clients créés depuis le dernier passage sortiraient de l’étoile.
docker compose exec -T airflow bash -c "cd /opt/airflow/dbt_stripe && dbt run --quiet --select +fact_transactions quarantine_transactions && dbt test --select quarantine_transactions fact_transactions 2>&1 | tail -1"
wh "SELECT transaction_id, reason FROM analytics.quarantine_transactions WHERE transaction_id >= 990000000"

echo "=== 5. Alertes enregistrées"
wh "SELECT raised_at::time(0), severity, source, left(message, 80) AS message FROM audit.alerts ORDER BY alert_id DESC LIMIT 4"
