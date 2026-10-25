#!/usr/bin/env bash
# ============================================================
# TEST DE CHARGE — de bout en bout (Livrable 5 §8.4)
# ------------------------------------------------------------
# Lance P exemplaires de l'API de paiement simulée en parallèle, chacun
# enregistrant N paiements (scoring en ligne compris), puis mesure :
#   1. le débit d'écriture dans l'OLTP (transactions par seconde) ;
#   2. le temps que met la voie flux à tout recopier dans la couche raw ;
#   3. le plus grand retard (lag) des consommateurs Kafka pendant la charge ;
#   4. la latence du scoring vue par l'API (médiane et p99 par exemplaire).
#
# Usage, depuis le dossier maquette (Git Bash) :
#   bash charge/test_de_charge.sh            # 6 exemplaires x 500 paiements
#   bash charge/test_de_charge.sh 8 1000     # 8 exemplaires x 1000 paiements
# ============================================================
set -euo pipefail

P="${1:-6}"
N="${2:-500}"
source .env
LOGS="$(mktemp -d)"

oltp_count() { docker compose exec -T oltp-db psql -U "$PG_ADMIN_USER" -d stripe_oltp -Atc "SELECT count(*) FROM oltp.transactions"; }
raw_count()  { docker compose exec -T warehouse psql -U "$PG_ADMIN_USER" -d warehouse -Atc "SELECT count(*) FROM raw.transactions"; }
raw_lag()    { docker compose exec -T redpanda rpk group describe sink-raw 2>/dev/null | awk '/TOTAL-LAG/ {print $2}'; }

before=$(oltp_count)
echo "Avant : ${before} transactions dans l'OLTP. Charge : ${P} exemplaires x ${N} paiements."

start=$(date +%s.%N)
for i in $(seq 1 "$P"); do
  MSYS_NO_PATHCONV=1 docker compose exec -T app-simulator \
    python /opt/app/app_simulator.py --once --n "$N" > "${LOGS}/api_${i}.log" 2>&1 &
done

max_lag=0
while [ "$(jobs -r | wc -l)" -gt 0 ]; do
  lag=$(raw_lag); lag=${lag:-0}
  [ "$lag" -gt "$max_lag" ] && max_lag=$lag
  sleep 1
done
wait
end=$(date +%s.%N)

after=$(oltp_count)
written=$((after - before))
write_s=$(awk -v a="$start" -v b="$end" 'BEGIN {printf "%.1f", b - a}')
echo "Écriture : ${written} transactions en ${write_s} s, soit $(awk -v w="$written" -v s="$write_s" 'BEGIN {printf "%.0f", w / s}') tx/s."

# La voie flux rattrape : on attend que raw contienne tout ce que l'OLTP contient
while [ "$(raw_count)" -lt "$after" ]; do
  lag=$(raw_lag); lag=${lag:-0}
  [ "$lag" -gt "$max_lag" ] && max_lag=$lag
  sleep 1
done
drained=$(date +%s.%N)
echo "Voie flux : couche raw à jour $(awk -v a="$end" -v b="$drained" 'BEGIN {printf "%.1f", b - a}') s après la fin des écritures ; retard maximal observé : ${max_lag} messages."

echo "Latence du scoring vue par chaque exemplaire de l'API :"
grep -ho "décisions.*" "${LOGS}"/api_*.log | sed 's/^/  /'
rm -rf "$LOGS"
