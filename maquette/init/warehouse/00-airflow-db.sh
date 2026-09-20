#!/bin/bash
# Base technique des métadonnées Airflow, hébergée sur la même instance que l'entrepôt
# (simplification de maquette : un seul serveur PostgreSQL pour ces deux usages).
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<'EOSQL'
CREATE DATABASE airflow;
EOSQL
