#!/bin/bash
# ============================================================
# Preuve de la clé de réconciliation : une même transaction suivie
# dans les trois systèmes (OLTP -> entrepôt -> MongoDB).
# Usage (depuis le dossier maquette, sous Git Bash) :
#   bash queries/04_reconciliation_transaction_id.sh
# ============================================================
set -e
source .env

TX_ID=$(docker compose exec -T warehouse psql -tA \
  "postgresql://data_analyst:${SERVICE_PASSWORD}@localhost/warehouse" \
  -c "SELECT transaction_id FROM analytics.fact_transactions WHERE fraud_score IS NOT NULL ORDER BY fraud_score DESC LIMIT 1")
echo "=== transaction_id retenu : ${TX_ID}"

echo "=== 1. OLTP (source de vérité, score réécrit)"
docker compose exec -T oltp-db psql \
  "postgresql://data_analyst:${SERVICE_PASSWORD}@localhost/stripe_oltp" \
  -c "SELECT transaction_id, amount, currency_code, status, fraud_score, created_at FROM oltp.transactions WHERE transaction_id = ${TX_ID}"

echo "=== 2. Entrepôt, couche analytics (dimension dégénérée)"
docker compose exec -T warehouse psql \
  "postgresql://data_analyst:${SERVICE_PASSWORD}@localhost/warehouse" \
  -c "SELECT transaction_id, amount_usd, status, fraud_score, is_fraud, model_version FROM analytics.fact_transactions WHERE transaction_id = ${TX_ID}"

echo "=== 3. MongoDB (features de risque, champ référencé)"
docker compose exec -T mongo mongosh --quiet \
  "mongodb://ml_reader:${SERVICE_PASSWORD}@localhost/stripe_nosql?authSource=stripe_nosql" \
  --eval "printjson(db.ml_features.findOne({transaction_id: ${TX_ID}}, {_id: 0}))"
