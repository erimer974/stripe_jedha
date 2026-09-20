#!/bin/bash
# ============================================================
# ENTREPÔT (rôle de Snowflake) — couches et rôles
#   raw        : données brutes chargées par l'ingestion (rôle d'Airbyte)
#   staging    : vues de préparation construites par dbt
#   analytics  : schéma en étoile + agrégats construits par dbt (Livrable 3)
#   ml         : scores de fraude écrits par le modèle (write-back)
#   audit      : rapports de conformité produits chaque jour
# ============================================================
set -e

psql -v ON_ERROR_STOP=1 -v svc_pw="$SERVICE_PASSWORD" \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'EOSQL'

-- ---- Rôles (un par étape du pipeline) ----
CREATE ROLE loader       LOGIN PASSWORD :'svc_pw';  -- ingestion : écrit raw
CREATE ROLE transformer  LOGIN PASSWORD :'svc_pw';  -- dbt : lit raw + ml, construit staging / analytics
CREATE ROLE ml_scorer    LOGIN PASSWORD :'svc_pw';  -- scoring : lit analytics, écrit ml
CREATE ROLE data_analyst LOGIN PASSWORD :'svc_pw';  -- BI : lit analytics uniquement
CREATE ROLE auditor      LOGIN PASSWORD :'svc_pw';  -- conformité : écrit audit

REVOKE CONNECT ON DATABASE warehouse FROM PUBLIC;
GRANT CONNECT ON DATABASE warehouse TO loader, transformer, ml_scorer, data_analyst, auditor;

-- ---- Schémas ----
CREATE SCHEMA raw;
CREATE SCHEMA staging   AUTHORIZATION transformer;
CREATE SCHEMA analytics AUTHORIZATION transformer;
CREATE SCHEMA ml;
CREATE SCHEMA audit;

-- ---- Couche RAW : copie fidèle des sources + horodatage de chargement ----
-- Pas de colonne email : l'ingestion n'y a pas accès (minimisation RGPD).
CREATE TABLE raw.transactions (
    transaction_id     BIGINT PRIMARY KEY,
    customer_id        BIGINT,
    merchant_id        BIGINT,
    payment_method_id  INT,
    currency_code      CHAR(3),
    amount             NUMERIC(12,2),
    ip_geolocation     VARCHAR(100),
    device_type        VARCHAR(30),
    status             VARCHAR(20),
    created_at         TIMESTAMPTZ,
    _loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE raw.customers (
    customer_id   BIGINT PRIMARY KEY,
    country_code  CHAR(2),
    created_at    TIMESTAMPTZ,
    _loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE raw.merchants (
    merchant_id   BIGINT PRIMARY KEY,
    name          VARCHAR(255),
    country_code  CHAR(2),
    created_at    TIMESTAMPTZ,
    _loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE raw.payment_methods (
    payment_method_id  INT PRIMARY KEY,
    label              VARCHAR(50),
    _loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Chargées depuis la source "Données de référence" (fichiers CSV)
CREATE TABLE raw.countries (
    country_code  CHAR(2) PRIMARY KEY,
    name          VARCHAR(100),
    _loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE raw.exchange_rates (
    currency_code  CHAR(3),
    rate_date      DATE,
    rate_to_usd    NUMERIC(12,6),
    _loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (currency_code, rate_date)
);

-- État des synchronisations (curseur incrémental par flux), comme le "state" d'un connecteur Airbyte
CREATE TABLE raw._sync_state (
    stream        TEXT PRIMARY KEY,
    sync_mode     TEXT NOT NULL,
    cursor_value  TEXT,
    rows_synced   INT,
    synced_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

GRANT USAGE ON SCHEMA raw TO loader, transformer;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA raw TO loader;
GRANT SELECT ON ALL TABLES IN SCHEMA raw TO transformer;

-- ---- Couche ML : scores de fraude (write-back du modèle) ----
CREATE TABLE ml.fraud_scores (
    transaction_id  BIGINT PRIMARY KEY,
    fraud_score     NUMERIC(5,4) NOT NULL CHECK (fraud_score BETWEEN 0 AND 1),
    model_version   TEXT NOT NULL,
    scored_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

GRANT USAGE ON SCHEMA ml TO ml_scorer, transformer;
GRANT SELECT, INSERT ON ml.fraud_scores TO ml_scorer;
GRANT SELECT ON ml.fraud_scores TO transformer;

-- ---- Couche ANALYTICS : lecture accordée table par table par dbt (config +grants) ----
GRANT USAGE ON SCHEMA analytics TO data_analyst, ml_scorer;

-- ---- Couche AUDIT ----
CREATE TABLE audit.compliance_reports (
    report_id   BIGSERIAL PRIMARY KEY,
    run_id      TEXT NOT NULL,
    checked_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    system      TEXT NOT NULL,                                   -- oltp | warehouse
    check_name  TEXT NOT NULL,
    blocking    BOOLEAN NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('PASS', 'FAIL', 'INFO')),
    detail      TEXT
);

-- Aucune colonne de donnée personnelle directe ou de carte dans l'entrepôt
CREATE VIEW audit.sensitive_columns_in_warehouse AS
SELECT n.nspname AS schema_name, c.relname AS table_name, att.attname AS column_name
FROM pg_attribute att
JOIN pg_class c ON c.oid = att.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname IN ('raw', 'staging', 'analytics', 'ml')
  AND c.relkind IN ('r', 'v', 'm')
  AND att.attnum > 0 AND NOT att.attisdropped
  AND att.attname ~* '(email|card_number|^pan$|cvv|cvc)';

-- L'analyste ne doit accéder ni aux données brutes ni aux scores bruts
CREATE VIEW audit.analyst_forbidden_access AS
SELECT s.schema_name
FROM unnest(ARRAY['raw', 'ml']) AS s(schema_name)
WHERE has_schema_privilege('data_analyst', s.schema_name, 'USAGE');

-- Fraîcheur de l'ingestion
CREATE VIEW audit.ingestion_freshness AS
SELECT stream, sync_mode, rows_synced, synced_at,
       round(extract(epoch FROM now() - synced_at) / 60) AS minutes_since_sync
FROM raw._sync_state;

GRANT USAGE ON SCHEMA audit TO auditor;
GRANT SELECT, INSERT ON audit.compliance_reports TO auditor;
GRANT USAGE ON SEQUENCE audit.compliance_reports_report_id_seq TO auditor;
GRANT SELECT ON audit.sensitive_columns_in_warehouse,
                audit.analyst_forbidden_access,
                audit.ingestion_freshness TO auditor;
EOSQL
