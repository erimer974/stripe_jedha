-- ============================================================
-- Livrable 3 — Modèle physique OLAP (Snowflake), architecture cible
-- Étude de cas Stripe · schéma en étoile · grain = 1 transaction
--
-- Ce script documente les couches et les tables de l'entrepôt.
-- En exploitation, les couches STAGING et ANALYTICS ne sont pas créées
-- à la main : dbt les construit et les teste à chaque passage du pipeline
-- (mêmes colonnes que ci-dessous ; modèles dans maquette/dbt_stripe/models).
-- En Snowflake, PRIMARY KEY et REFERENCES sont déclaratives (non vérifiées) :
-- elles documentent le modèle ; l'intégrité est vérifiée par les tests dbt.
--
-- Maquette : mêmes couches et mêmes tables dans PostgreSQL
-- (maquette/init/warehouse/01-layers-and-roles.sh et modèles dbt).
-- ============================================================

CREATE DATABASE IF NOT EXISTS STRIPE_DWH;
USE DATABASE STRIPE_DWH;

-- Calcul séparé du stockage : un entrepôt virtuel par usage, dimensionné et
-- suspendu indépendamment (le chargement ne ralentit pas la BI).
CREATE WAREHOUSE IF NOT EXISTS WH_LOADING      WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
CREATE WAREHOUSE IF NOT EXISTS WH_TRANSFORMING WAREHOUSE_SIZE = 'SMALL'  AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
CREATE WAREHOUSE IF NOT EXISTS WH_BI           WAREHOUSE_SIZE = 'SMALL'  AUTO_SUSPEND = 60 AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 3;   -- montée en charge horizontale aux heures de pointe

CREATE SCHEMA IF NOT EXISTS RAW;        -- copie brute des sources (Airbyte)
CREATE SCHEMA IF NOT EXISTS STAGING;    -- vues de préparation (dbt)
CREATE SCHEMA IF NOT EXISTS ANALYTICS;  -- étoile + pré-agrégats (dbt)
CREATE SCHEMA IF NOT EXISTS ML;         -- scores écrits par le modèle de fraude
CREATE SCHEMA IF NOT EXISTS AUDIT;      -- rapports de conformité

-- ============================================================
-- RAW : copie fidèle des sources + horodatage de chargement
-- Aucune colonne email : l'ingestion n'y a pas accès (minimisation RGPD).
-- ============================================================

CREATE TABLE IF NOT EXISTS RAW.TRANSACTIONS (
    transaction_id     NUMBER(19,0) PRIMARY KEY,
    customer_id        NUMBER(19,0),
    merchant_id        NUMBER(19,0),
    payment_method_id  NUMBER(10,0),
    currency_code      CHAR(3),
    amount             NUMBER(12,2),
    ip_geolocation     VARCHAR(100),
    device_type        VARCHAR(30),
    status             VARCHAR(20),
    created_at         TIMESTAMP_TZ,
    _loaded_at         TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.CUSTOMERS (
    customer_id   NUMBER(19,0) PRIMARY KEY,
    country_code  CHAR(2),
    created_at    TIMESTAMP_TZ,
    _loaded_at    TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.MERCHANTS (
    merchant_id   NUMBER(19,0) PRIMARY KEY,
    name          VARCHAR(255),
    country_code  CHAR(2),
    created_at    TIMESTAMP_TZ,
    _loaded_at    TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.PAYMENT_METHODS (
    payment_method_id  NUMBER(10,0) PRIMARY KEY,
    label              VARCHAR(50),
    _loaded_at         TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.COUNTRIES (          -- source : données de référence
    country_code  CHAR(2) PRIMARY KEY,
    name          VARCHAR(100),
    _loaded_at    TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.EXCHANGE_RATES (     -- source : données de référence
    currency_code  CHAR(3),
    rate_date      DATE,
    rate_to_usd    NUMBER(12,6),
    _loaded_at     TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (currency_code, rate_date)
);

-- ============================================================
-- ML : scores de fraude (write-back du modèle)
-- ============================================================

CREATE TABLE IF NOT EXISTS ML.FRAUD_SCORES (
    transaction_id  NUMBER(19,0) PRIMARY KEY,
    fraud_score     NUMBER(5,4)  NOT NULL,     -- entre 0 et 1 (vérifié par dbt test)
    model_version   VARCHAR      NOT NULL,     -- version MLflow du modèle « champion »
    scored_at       TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- ANALYTICS : schéma en étoile (construit par dbt)
-- ============================================================

-- Quand ? Table de dates continue (date spine), clé intelligente AAAAMMJJ
CREATE OR REPLACE TABLE ANALYTICS.DIM_DATE (
    date_key      NUMBER(8,0)  PRIMARY KEY,
    full_date     DATE         NOT NULL,
    year          NUMBER(4,0)  NOT NULL,
    quarter       NUMBER(1,0)  NOT NULL,
    month         NUMBER(2,0)  NOT NULL,
    day           NUMBER(2,0)  NOT NULL,
    day_of_week   NUMBER(1,0)  NOT NULL      -- 1 = lundi (ISO)
);

-- Qui paie ? Dénormalisée (pays en clair) ; segment de valeur recalculé à chaque passage
CREATE OR REPLACE TABLE ANALYTICS.DIM_CUSTOMER (
    customer_key  NUMBER(19,0) PRIMARY KEY,  -- clé de substitution
    customer_id   NUMBER(19,0) NOT NULL,     -- clé naturelle (OLTP)
    country       VARCHAR(100),
    segment       VARCHAR(20)                -- high_value | regular | occasional
);

-- Chez qui ?
CREATE OR REPLACE TABLE ANALYTICS.DIM_MERCHANT (
    merchant_key  NUMBER(19,0) PRIMARY KEY,
    merchant_id   NUMBER(19,0) NOT NULL,
    name          VARCHAR(255),
    country       VARCHAR(100)
);

-- Comment ? Référence du moyen de paiement, jamais de numéro de carte
CREATE OR REPLACE TABLE ANALYTICS.DIM_PAYMENT_METHOD (
    payment_method_key NUMBER(10,0) PRIMARY KEY,
    label              VARCHAR(50)  NOT NULL
);

-- Faits : une ligne = une transaction
CREATE OR REPLACE TABLE ANALYTICS.FACT_TRANSACTIONS (
    transaction_key     NUMBER(19,0) PRIMARY KEY,
    date_key            NUMBER(8,0)  NOT NULL REFERENCES ANALYTICS.DIM_DATE(date_key),
    customer_key        NUMBER(19,0) NOT NULL REFERENCES ANALYTICS.DIM_CUSTOMER(customer_key),
    merchant_key        NUMBER(19,0) NOT NULL REFERENCES ANALYTICS.DIM_MERCHANT(merchant_key),
    payment_method_key  NUMBER(10,0) NOT NULL REFERENCES ANALYTICS.DIM_PAYMENT_METHOD(payment_method_key),
    transaction_id      NUMBER(19,0) NOT NULL UNIQUE,  -- dimension dégénérée : réconciliation OLTP / NoSQL
    currency_code       CHAR(3),                       -- dimension dégénérée : devise d'origine
    amount_usd          NUMBER(14,2) NOT NULL,         -- mesure additive, convertie au taux du jour
    status              VARCHAR(20)  NOT NULL,         -- succeeded | failed | refunded
    device_type         VARCHAR(30),
    fraud_score         NUMBER(5,4),                   -- NULL tant que la transaction n'est pas scorée
    is_fraud            BOOLEAN,                       -- fraud_score >= 0,5
    model_version       VARCHAR,                       -- version du modèle ayant produit le score
    created_at          TIMESTAMP_TZ NOT NULL
)
CLUSTER BY (date_key);   -- micro-partitions triées par date : les requêtes sur une période n'en lisent qu'une fraction

-- Pré-agrégat : revenus et fraude par jour et par commerçant, lu directement par la BI
CREATE OR REPLACE TABLE ANALYTICS.AGG_REVENUE_DAILY (
    date_key           NUMBER(8,0)  NOT NULL REFERENCES ANALYTICS.DIM_DATE(date_key),
    merchant_key       NUMBER(19,0) NOT NULL REFERENCES ANALYTICS.DIM_MERCHANT(merchant_key),
    total_revenue_usd  NUMBER(16,2) NOT NULL,   -- transactions réussies uniquement
    nb_transactions    NUMBER(12,0) NOT NULL,
    nb_scored          NUMBER(12,0) NOT NULL,
    nb_flagged_fraud   NUMBER(12,0) NOT NULL,
    PRIMARY KEY (date_key, merchant_key)
);

-- ============================================================
-- AUDIT : rapports de conformité historisés (livrable 6)
-- ============================================================

CREATE TABLE IF NOT EXISTS AUDIT.COMPLIANCE_REPORTS (
    report_id   NUMBER(19,0) AUTOINCREMENT PRIMARY KEY,
    run_id      VARCHAR      NOT NULL,          -- exécution du DAG stripe_compliance_daily
    checked_at  TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    system      VARCHAR      NOT NULL,          -- oltp | warehouse | nosql
    check_name  VARCHAR      NOT NULL,
    blocking    BOOLEAN      NOT NULL,
    status      VARCHAR      NOT NULL,          -- PASS | FAIL | INFO
    detail      VARCHAR
);

-- ============================================================
-- Construction de l'étoile par dbt (résumé ; SQL complet dans maquette/dbt_stripe/models)
--   STAGING.STG_TRANSACTIONS (vue) : RAW.TRANSACTIONS
--       + conversion USD au dernier taux connu à la date de la transaction
--       + score et version rattachés depuis ML.FRAUD_SCORES
--   ANALYTICS.DIM_* : depuis RAW (pays en clair ; segment calculé sur le cumul en USD)
--   ANALYTICS.FACT_TRANSACTIONS : STG_TRANSACTIONS joint aux dimensions
--   ANALYTICS.AGG_REVENUE_DAILY : agrégation de FACT_TRANSACTIONS
-- ============================================================
