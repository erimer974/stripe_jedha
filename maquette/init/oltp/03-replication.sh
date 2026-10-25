#!/bin/bash
# ============================================================
# SOURCE OLTP — haute disponibilité (Livrable 2 §6, Livrable 5 §9)
#   1. rôle « replicator » : utilisé par le réplica pour recopier le journal (WAL)
#   2. slot logique de Debezium créé en mode « failover » (PostgreSQL 17) :
#      il est recopié sur le réplica et survit à la bascule, donc la capture
#      des changements reprend sur le nouveau primaire sans rien perdre.
# La réplication synchrone (RPO 0) est activée et désactivée par le gardien
# (ha/failover_watchdog.py) selon que le réplica est présent ou non.
# Exécuté une seule fois, à la création du premier primaire.
# ============================================================
set -e

psql -v ON_ERROR_STOP=1 -v repl_pw="$SERVICE_PASSWORD" \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'EOSQL'
CREATE ROLE replicator LOGIN REPLICATION PASSWORD :'repl_pw';
-- connexion à la base nécessaire à la synchronisation des slots (sync_replication_slots)
GRANT CONNECT ON DATABASE stripe_oltp TO replicator;

-- slot_name, plugin, temporary, two_phase, failover
SELECT pg_create_logical_replication_slot('cdc_slot', 'pgoutput', false, false, true);
EOSQL

# Le réplica se connecte en mode réplication depuis le réseau des conteneurs
echo "host replication replicator all scram-sha-256" >> "$PGDATA/pg_hba.conf"
