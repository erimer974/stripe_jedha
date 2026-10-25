#!/bin/bash
# ============================================================
# Démarrage d'un nœud du cluster OLTP (équivalent simplifié de Patroni)
# ------------------------------------------------------------
# Deux nœuds, oltp-db et oltp-replica. Le fichier partagé /ha/leader dit
# lequel est le primaire (écrit par le gardien lors d'une bascule).
#   - si ce nœud est le primaire : il démarre normalement ;
#   - sinon : il devient réplica du primaire. S'il n'est pas déjà son réplica
#     (premier démarrage, ou ANCIEN primaire qui revient après une panne), il
#     efface ses données et les recopie entièrement depuis le primaire
#     (pg_basebackup). Un ancien primaire ne peut donc jamais redémarrer en
#     primaire : pas de « cerveau divisé » (deux primaires à la fois).
# ============================================================
set -euo pipefail

NODE="$NODE_NAME"
SLOT="${NODE//-/_}"                                  # nom du slot et de l'application : oltp_db, oltp_replica
LEADER="$(cat /ha/leader 2>/dev/null || echo "$INITIAL_LEADER")"
PG_ARGS=(-c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10
         -c hot_standby=on -c hot_standby_feedback=on -c sync_replication_slots=on)

if [ "$LEADER" = "$NODE" ]; then
  echo "[$NODE] démarrage en PRIMAIRE"
  exec docker-entrypoint.sh postgres "${PG_ARGS[@]}"
fi

echo "[$NODE] démarrage en RÉPLICA de $LEADER"
export PGPASSWORD="$SERVICE_PASSWORD"
until pg_isready -q -h "$LEADER" -U replicator -d stripe_oltp; do sleep 2; done

mkdir -p "$PGDATA" && chown postgres:postgres "$PGDATA" && chmod 700 "$PGDATA"
if [ ! -f "$PGDATA/standby.signal" ] || ! grep -q "host=$LEADER " "$PGDATA/postgresql.auto.conf"; then
  echo "[$NODE] copie complète des données depuis $LEADER (pg_basebackup)"
  find "$PGDATA" -mindepth 1 -delete
  psql -h "$LEADER" -U replicator -d stripe_oltp -Atc \
    "SELECT pg_drop_replication_slot('$SLOT') FROM pg_replication_slots WHERE slot_name = '$SLOT'" > /dev/null
  gosu postgres pg_basebackup -h "$LEADER" -U replicator -D "$PGDATA" -X stream -C -S "$SLOT" -R
  # dbname : nécessaire pour que le réplica recopie aussi le slot logique de Debezium
  cat >> "$PGDATA/postgresql.auto.conf" <<EOF
primary_conninfo = 'host=$LEADER port=5432 user=replicator password=$SERVICE_PASSWORD application_name=$SLOT dbname=stripe_oltp'
primary_slot_name = '$SLOT'
EOF
fi

exec gosu postgres postgres "${PG_ARGS[@]}"
