#!/usr/bin/env bash
# ============================================================
# FLEXIBILITÉ — un nouveau champ ne casse rien (Livrable 5 §8.5)
# ------------------------------------------------------------
# 1. On ajoute une colonne « channel » (canal de paiement) aux transactions,
#    pendant que le pipeline tourne.
# 2. Debezium la publie automatiquement : elle apparaît dans les messages Kafka.
# 3. Les consommateurs ignorent ce qu'ils ne connaissent pas encore : la couche
#    raw et l'étoile continuent de se remplir, sans erreur. On ajoutera le champ
#    à l'entrepôt le jour où un usage le demande.
# 4. On ajoute aussi une colonne « phone » aux clients : elle n'est PAS publiée,
#    car la publication de la table clients liste ses colonnes une à une
#    (protection des données personnelles par défaut).
#
# Usage, depuis le dossier maquette (Git Bash) :
#   bash queries/06_evolution_schema.sh            # démonstration
#   bash queries/06_evolution_schema.sh --annuler  # retire les deux colonnes
# ============================================================
set -euo pipefail
source .env
admin() { docker compose exec -T oltp-db psql -U "$PG_ADMIN_USER" -d stripe_oltp -Atc "$1"; }

if [ "${1:-}" = "--annuler" ]; then
  admin "ALTER TABLE oltp.transactions DROP COLUMN IF EXISTS channel"
  admin "ALTER TABLE oltp.customers DROP COLUMN IF EXISTS phone"
  echo "Colonnes retirées."
  exit 0
fi

echo "=== 1. Ajout des colonnes, pipeline en marche"
admin "ALTER TABLE oltp.transactions ADD COLUMN IF NOT EXISTS channel VARCHAR(20) NOT NULL DEFAULT 'web'"
admin "ALTER TABLE oltp.customers ADD COLUMN IF NOT EXISTS phone VARCHAR(30)"
raw_before=$(docker compose exec -T warehouse psql -U "$PG_ADMIN_USER" -d warehouse -Atc "SELECT count(*) FROM raw.transactions")

echo "=== 2. Colonnes publiées par la base (publication CDC)"
admin "SELECT tablename || ' : ' || array_to_string(attnames, ', ') FROM pg_publication_tables
       WHERE pubname = 'cdc_publication' AND tablename IN ('transactions', 'customers')"

echo "=== 3. Attente du prochain lot de paiements (40 s)"
sleep 40

echo "=== 4. Dernier message Kafka de cdc.oltp.transactions : le champ channel y figure"
docker compose exec -T redpanda rpk topic consume cdc.oltp.transactions -o -1 -n 1 -f '%v\n' | head -c 400; echo

echo "=== 5. La voie flux continue sans erreur"
raw_after=$(docker compose exec -T warehouse psql -U "$PG_ADMIN_USER" -d warehouse -Atc "SELECT count(*) FROM raw.transactions")
echo "transactions dans raw : ${raw_before} -> ${raw_after}"
failures=$(docker compose logs --since 60s stream-processor | grep -c "échec" || true)
echo "échecs du consommateur depuis 1 min : ${failures}"
