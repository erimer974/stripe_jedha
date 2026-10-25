#!/usr/bin/env bash
# ============================================================
# TEST DE BASCULE — panne brutale du primaire OLTP (Livrable 5 §9)
# ------------------------------------------------------------
# 1. la sonde enregistre des paiements un par un pendant 70 s ;
# 2. au bout de 15 s, le primaire est arrêté brutalement (docker kill) ;
# 3. le gardien détecte la panne et promeut le réplica ;
# 4. on vérifie : RPO (aucun paiement confirmé perdu), RTO (interruption
#    des écritures), reprise de la capture des changements, couche raw complète.
#
# Usage, depuis le dossier maquette (Git Bash) :  bash ha/test_bascule.sh
# Puis, pour réintégrer l'ancien primaire comme réplica : docker compose start <ancien primaire>
# ============================================================
set -euo pipefail
source .env

leader=$(MSYS_NO_PATHCONV=1 docker compose exec -T failover-watchdog cat /ha/leader 2>/dev/null || true)
leader=${leader:-oltp-db}
echo "Primaire actuel : ${leader}. Lancement de la sonde (70 s)."

MSYS_NO_PATHCONV=1 docker compose exec -T app-simulator \
  python /opt/ha/sonde_bascule.py --duree 70 --sortie /tmp/sonde.json &
sleep 15
echo "$(date +%T) PANNE : arrêt brutal de ${leader}"
docker compose kill "$leader" > /dev/null 2>&1
wait

new_leader=$(MSYS_NO_PATHCONV=1 docker compose exec -T failover-watchdog cat /ha/leader)
echo
echo "=== Gardien"
docker compose logs --since 90s failover-watchdog | grep -E "BASCULE|réalignée|relancée|isolé|ASYNCHRONE" | sed 's/^failover-watchdog-1  | /  /'

echo "=== RPO : les paiements confirmés sont-ils tous dans le nouveau primaire (${new_leader}) ?"
ids=$(MSYS_NO_PATHCONV=1 docker compose exec -T app-simulator python -c \
  "import json; print(','.join(map(str, json.load(open('/tmp/sonde.json'))['ids'])))")
confirmed=$(echo "$ids" | tr ',' '\n' | grep -c . || true)
missing=$(docker compose exec -T "$new_leader" psql -U "$PG_ADMIN_USER" -d stripe_oltp -Atc \
  "SELECT count(*) FROM unnest('{${ids}}'::bigint[]) AS id
   WHERE NOT EXISTS (SELECT 1 FROM oltp.transactions t WHERE t.transaction_id = id)")
echo "  ${confirmed} paiements confirmés, ${missing} absents du nouveau primaire"

echo "=== Capture des changements et couche raw"
oltp=$(docker compose exec -T "$new_leader" psql -U "$PG_ADMIN_USER" -d stripe_oltp -Atc "SELECT count(*) FROM oltp.transactions")
for i in $(seq 1 30); do
  raw=$(docker compose exec -T warehouse psql -U "$PG_ADMIN_USER" -d warehouse -Atc "SELECT count(*) FROM raw.transactions")
  [ "$raw" -ge "$oltp" ] && break
  sleep 2
done
dups=$(docker compose exec -T warehouse psql -U "$PG_ADMIN_USER" -d warehouse -Atc "SELECT count(*) - count(DISTINCT transaction_id) FROM raw.transactions")
echo "  OLTP : ${oltp} ; raw : ${raw} ; doublons dans raw : ${dups}"
echo "  connecteur : $(curl -s localhost:8083/connectors/oltp-cdc/status | grep -o '"tasks":\[{"id":0,"state":"[A-Z]*"' | grep -o '[A-Z]*"$' | tr -d '"')"
echo
echo "Pour réintégrer ${leader} comme réplica : docker compose start ${leader}"
