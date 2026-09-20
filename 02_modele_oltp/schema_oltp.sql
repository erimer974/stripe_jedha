-- ============================================================
-- Livrable 2 — Modèle physique OLTP (PostgreSQL), architecture cible
-- Étude de cas Stripe · méthode MERISE (MCD → MLD → MPD) · 3NF · ACID
--
-- Ordre du script : tables de référence (sans dépendance), entités
-- principales (clients, commerçants), puis table centrale `transactions`.
-- Le script se rejoue tel quel sur une base vide.
--
-- La maquette exécute ce même modèle : maquette/init/oltp/01-schema.sql.
-- Réplication, bascule et partitionnement relèvent du déploiement cible
-- (voir la fin du fichier et 02_modele_oltp/README.md).
-- ============================================================

CREATE SCHEMA IF NOT EXISTS oltp;
SET search_path TO oltp;

-- ---- Tables de référence ----
-- Données stables ou à évolution lente. Un libellé ne vit qu'à un seul endroit (3NF).

CREATE TABLE countries (
    country_code  CHAR(2)      PRIMARY KEY,
    name          VARCHAR(100) NOT NULL
);

CREATE TABLE currencies (
    currency_code CHAR(3)      PRIMARY KEY,
    name          VARCHAR(100) NOT NULL
);

-- Entité faible : un taux n'existe que pour une devise et une date (clé composite)
CREATE TABLE exchange_rates (
    currency_code CHAR(3)        NOT NULL REFERENCES currencies(currency_code),
    rate_date     DATE           NOT NULL,
    rate_to_usd   DECIMAL(12,6)  NOT NULL CHECK (rate_to_usd > 0),
    PRIMARY KEY (currency_code, rate_date)
);

CREATE TABLE payment_methods (
    payment_method_id SERIAL       PRIMARY KEY,
    label             VARCHAR(50)  NOT NULL UNIQUE          -- carte, virement, wallet
);

-- ---- Entités principales ----

CREATE TABLE customers (
    customer_id   BIGSERIAL    PRIMARY KEY,
    email         VARCHAR(255) UNIQUE NOT NULL,             -- donnée personnelle (RGPD)
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
-- Une ligne = un paiement, un remboursement ou une rétrofacturation.
-- Aucun numéro de carte (PAN) : le moyen de paiement est une référence,
-- la carte reste tokenisée chez le prestataire (PCI-DSS).

CREATE TABLE transactions (
    transaction_id      BIGSERIAL     PRIMARY KEY,          -- clé de réconciliation OLTP / OLAP / NoSQL
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

-- ---- Index ----
-- Uniquement sur les colonnes filtrées en permanence ; les clés primaires
-- et les colonnes UNIQUE sont déjà indexées par PostgreSQL.

CREATE INDEX idx_tx_customer   ON transactions(customer_id);     -- historique d'un client
CREATE INDEX idx_tx_merchant   ON transactions(merchant_id);     -- historique d'un commerçant
CREATE INDEX idx_tx_created    ON transactions(created_at);      -- fenêtres récentes + extraction incrémentale
CREATE INDEX idx_tx_unscored   ON transactions(transaction_id) WHERE fraud_score IS NULL;  -- index partiel : transactions à scorer
CREATE INDEX idx_cust_created  ON customers(created_at);         -- extraction incrémentale
CREATE INDEX idx_merch_created ON merchants(created_at);         -- extraction incrémentale

-- ---- Données de référence initiales ----

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

-- ============================================================
-- Déploiement cible : volume et haute disponibilité
-- (non exécuté dans la maquette, qui tourne sur une seule instance)
--
-- 1. Réplication et bascule
--    Un primaire et un réplica synchrone (synchronous_commit = on) : une
--    transaction validée existe sur les deux nœuds. En cas de panne du
--    primaire, le réplica est promu (bascule automatique par le service
--    managé, par exemple Amazon RDS Multi-AZ). Des réplicas asynchrones
--    supplémentaires absorbent les lectures, dont l'extraction Airbyte.
--
-- 2. Reprise après sinistre
--    Sauvegarde complète quotidienne + archivage continu des journaux WAL :
--    restauration à un instant précis (PITR). Sauvegardes chiffrées,
--    copiées dans une autre région.
--
-- 3. Partitionnement de `transactions` (quand le volume l'exige)
--    Partitionnement par plage mensuelle sur created_at. PostgreSQL impose
--    que la clé primaire contienne la clé de partition :
--
--      CREATE TABLE transactions ( ...mêmes colonnes...,
--          PRIMARY KEY (transaction_id, created_at)
--      ) PARTITION BY RANGE (created_at);
--      CREATE TABLE transactions_2026_09 PARTITION OF transactions
--          FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
--
--    transaction_id reste unique (séquence). Les requêtes récentes et
--    l'extraction incrémentale ne lisent que la partition du mois, et les
--    partitions anciennes s'archivent sans toucher au reste.
-- ============================================================
