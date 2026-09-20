-- ============================================================
-- SOURCE OLTP — schéma normalisé (Livrable 2, MERISE / 3NF / ACID)
-- Base "stripe_oltp", schéma "oltp". Modèle identique au modèle cible 02_modele_oltp/schema_oltp.sql
-- (une seule instance : ni réplication ni partitionnement dans la maquette).
-- ============================================================

CREATE SCHEMA IF NOT EXISTS oltp;
SET search_path TO oltp;

-- ---- Tables de référence (nécessaires aux clés étrangères de l'application) ----

CREATE TABLE countries (
    country_code  CHAR(2)      PRIMARY KEY,
    name          VARCHAR(100) NOT NULL
);

CREATE TABLE currencies (
    currency_code CHAR(3)      PRIMARY KEY,
    name          VARCHAR(100) NOT NULL
);

CREATE TABLE exchange_rates (
    currency_code CHAR(3)        NOT NULL REFERENCES currencies(currency_code),
    rate_date     DATE           NOT NULL,
    rate_to_usd   DECIMAL(12,6)  NOT NULL CHECK (rate_to_usd > 0),
    PRIMARY KEY (currency_code, rate_date)
);

CREATE TABLE payment_methods (
    payment_method_id SERIAL       PRIMARY KEY,
    label             VARCHAR(50)  NOT NULL UNIQUE
);

-- ---- Entités principales ----

CREATE TABLE customers (
    customer_id   BIGSERIAL    PRIMARY KEY,
    email         VARCHAR(255) UNIQUE NOT NULL,          -- donnée personnelle (RGPD)
    country_code  CHAR(2)      REFERENCES countries(country_code),
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE merchants (
    merchant_id   BIGSERIAL    PRIMARY KEY,
    name          VARCHAR(255) NOT NULL,
    country_code  CHAR(2)      REFERENCES countries(country_code),
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- ---- Entité centrale ----
-- Aucun numéro de carte (PAN) n'est stocké : le moyen de paiement est une référence (PCI-DSS).

CREATE TABLE transactions (
    transaction_id      BIGSERIAL     PRIMARY KEY,        -- clé de réconciliation OLTP / OLAP / NoSQL
    customer_id         BIGINT        NOT NULL REFERENCES customers(customer_id),
    merchant_id         BIGINT        NOT NULL REFERENCES merchants(merchant_id),
    payment_method_id   INT           NOT NULL REFERENCES payment_methods(payment_method_id),
    currency_code       CHAR(3)       NOT NULL REFERENCES currencies(currency_code),
    amount              DECIMAL(12,2) NOT NULL CHECK (amount > 0),
    ip_geolocation      VARCHAR(100),
    device_type         VARCHAR(30),
    status              VARCHAR(20)   NOT NULL CHECK (status IN ('succeeded', 'failed', 'refunded')),
    fraud_score         NUMERIC(5,4)  CHECK (fraud_score BETWEEN 0 AND 1),  -- réécrit par le scoring (write-back)
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- ---- Index (optimisation transactionnelle + extraction incrémentale par created_at) ----

CREATE INDEX idx_tx_customer   ON transactions(customer_id);
CREATE INDEX idx_tx_merchant   ON transactions(merchant_id);
CREATE INDEX idx_tx_created    ON transactions(created_at);
CREATE INDEX idx_tx_unscored   ON transactions(transaction_id) WHERE fraud_score IS NULL;
CREATE INDEX idx_cust_created  ON customers(created_at);
CREATE INDEX idx_merch_created ON merchants(created_at);

-- ---- Données de référence de l'application ----

INSERT INTO countries (country_code, name) VALUES
    ('US', 'United States'), ('FR', 'France'), ('DE', 'Germany'),
    ('GB', 'United Kingdom'), ('JP', 'Japan');

INSERT INTO currencies (currency_code, name) VALUES
    ('USD', 'US Dollar'), ('EUR', 'Euro'), ('GBP', 'Pound Sterling'), ('JPY', 'Yen');

INSERT INTO exchange_rates (currency_code, rate_date, rate_to_usd) VALUES
    ('USD', '2026-01-01', 1.0), ('EUR', '2026-01-01', 1.08),
    ('GBP', '2026-01-01', 1.27), ('JPY', '2026-01-01', 0.0067);

INSERT INTO payment_methods (label) VALUES ('carte'), ('virement'), ('wallet');

RESET search_path;
