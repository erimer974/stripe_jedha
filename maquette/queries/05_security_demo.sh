#!/bin/bash
# ============================================================
# Démonstration du moindre privilège : chaque refus ci-dessous est ATTENDU.
# Usage (depuis le dossier maquette, sous Git Bash) :
#   bash queries/05_security_demo.sh
# ============================================================
source .env
OLTP_ANALYST="postgresql://data_analyst:${SERVICE_PASSWORD}@localhost/stripe_oltp"
WH_ANALYST="postgresql://data_analyst:${SERVICE_PASSWORD}@localhost/warehouse"

echo "=== 1. L'analyste lit l'email en clair dans l'OLTP -> refus attendu (RGPD)"
docker compose exec -T oltp-db psql "$OLTP_ANALYST" -c "SELECT email FROM oltp.customers LIMIT 1"

echo "=== 2. ... mais il lit la vue pseudonymisée -> autorisé"
docker compose exec -T oltp-db psql "$OLTP_ANALYST" -c "SELECT customer_id, email_hash FROM oltp.customers_pseudo LIMIT 3"

echo "=== 3. L'analyste lit la couche raw de l'entrepôt -> refus attendu"
docker compose exec -T warehouse psql "$WH_ANALYST" -c "SELECT * FROM raw.transactions LIMIT 1"

echo "=== 4. Le scoring tente de modifier un montant -> refus attendu (droit limité à la colonne fraud_score)"
docker compose exec -T oltp-db psql "postgresql://fraud_writer:${SERVICE_PASSWORD}@localhost/stripe_oltp" \
  -c "UPDATE oltp.transactions SET amount = 1 WHERE transaction_id = 1"

echo "=== 5. Dernier rapport de conformité (rôle auditor)"
docker compose exec -T warehouse psql "postgresql://auditor:${SERVICE_PASSWORD}@localhost/warehouse" \
  -c "SELECT system, check_name, blocking, status, detail FROM audit.compliance_reports WHERE run_id = (SELECT run_id FROM audit.compliance_reports ORDER BY checked_at DESC LIMIT 1) ORDER BY blocking DESC, system, check_name"
