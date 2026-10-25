-- ============================================================
-- Livrable 2 — Modèle physique OLTP (PostgreSQL), architecture cible
-- Étude de cas Stripe · méthode MERISE (MCD → MLD → MPD) · 3NF · ACID
--
-- Ordre du script : tables de référence (sans dépendance), entités
-- principales (clients, commerçants), puis table centrale `transactions`.
-- Le script se rejoue tel quel sur une base vide.
--
-- La maquette exécute ce même modèle : maquette/init/oltp/01-schema.sql.
-- Le partitionnement est exécuté par la maquette ; réplication, bascule et
-- sharding relèvent du déploiement cible
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
--
-- Partitionnée par MOIS sur created_at DÈS LE DÉPART (≈ 7 To par an, livrable 0 §3.3) :
--   * les requêtes récentes ne lisent que la partition du mois (élagage des partitions) ;
--   * un mois de plus de 13 mois se détache et s'archive sans bloquer la table.
-- PostgreSQL impose que la clé primaire contienne la clé de partition.

CREATE TABLE transactions (
    transaction_id      BIGSERIAL     NOT NULL,             -- clé de réconciliation OLTP / OLAP / NoSQL
    customer_id         BIGINT        NOT NULL REFERENCES customers(customer_id),
    merchant_id         BIGINT        NOT NULL REFERENCES merchants(merchant_id),
    payment_method_id   INT           NOT NULL REFERENCES payment_methods(payment_method_id),
    currency_code       CHAR(3)       NOT NULL REFERENCES currencies(currency_code),
    amount              DECIMAL(12,2) NOT NULL CHECK (amount > 0),
    ip_geolocation      VARCHAR(100),
    device_type         VARCHAR(30),
    status              VARCHAR(20)   NOT NULL CHECK (status IN ('succeeded', 'failed', 'refunded')),
    fraud_score         NUMERIC(5,4)  CHECK (fraud_score BETWEEN 0 AND 1),  -- écrit au paiement (scoring en ligne)
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (transaction_id, created_at)
) PARTITION BY RANGE (created_at);

-- Filet de sécurité : une transaction hors des mois créés n'est jamais refusée.
-- Elle doit rester vide (contrôlée par la surveillance de capacité).
CREATE TABLE transactions_default PARTITION OF transactions DEFAULT;

-- Crée les partitions du mois courant et des `months_ahead` mois suivants (idempotent).
-- Appelée à l'installation, puis chaque heure par la surveillance de capacité.
-- Cible : l'extension pg_partman fait la même chose, archivage compris.
CREATE FUNCTION ensure_monthly_partitions(months_ahead INT DEFAULT 3) RETURNS INT
LANGUAGE plpgsql SECURITY DEFINER SET search_path = oltp AS $$
DECLARE
    month_start DATE;
    part_name   TEXT;
    created     INT := 0;
BEGIN
    FOR i IN 0..months_ahead LOOP
        month_start := (date_trunc('month', now()) + make_interval(months => i))::date;
        part_name   := format('transactions_%s', to_char(month_start, 'YYYY_MM'));
        IF to_regclass('oltp.' || part_name) IS NULL THEN
            EXECUTE format('CREATE TABLE oltp.%I PARTITION OF oltp.transactions FOR VALUES FROM (%L) TO (%L)',
                           part_name, month_start, (month_start + INTERVAL '1 month')::date);
            created := created + 1;
        END IF;
    END LOOP;
    RETURN created;
END $$;

SELECT ensure_monthly_partitions(3);

-- ---- Index ----
-- Uniquement sur les colonnes filtrées en permanence ; les clés primaires
-- et les colonnes UNIQUE sont déjà indexées par PostgreSQL. Créés sur la table
-- partitionnée, ils existent automatiquement sur chaque partition, présente et future.

CREATE INDEX idx_tx_customer   ON transactions(customer_id);     -- historique d'un client
CREATE INDEX idx_tx_merchant   ON transactions(merchant_id);     -- historique d'un commerçant
CREATE INDEX idx_tx_created    ON transactions(created_at);      -- fenêtres récentes
CREATE INDEX idx_tx_unscored   ON transactions(transaction_id) WHERE fraud_score IS NULL;  -- index partiel : transactions à scorer (rattrapage)
CREATE INDEX idx_cust_created  ON customers(created_at);         -- nouveaux clients
CREATE INDEX idx_merch_created ON merchants(created_at);         -- nouveaux commerçants

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
-- Déploiement cible : volume, montée en charge et haute disponibilité
-- (le partitionnement ci-dessus est exécuté aussi dans la maquette ; le reste
--  demande plusieurs serveurs)
--
-- 1. Réplication et bascule (livrable 5 §9)
--    3 nœuds sur 3 zones, réplication synchrone par quorum :
--      synchronous_standby_names = 'ANY 1 (replica_b, replica_c)'
--    Une transaction validée existe sur au moins 2 nœuds (RPO 0). Patroni
--    (verrou dans etcd) promeut un réplica si le primaire disparaît ; HAProxy
--    envoie les clients au primaire. PostgreSQL 17 : le slot de Debezium est
--    créé en mode failover pour survivre à la bascule.
--
-- 2. Reprise après sinistre
--    Sauvegarde complète quotidienne + archivage continu des journaux WAL :
--    restauration à un instant précis (PITR). Sauvegardes chiffrées,
--    copiées dans une autre région.
--
-- 3. Archivage des partitions (> 13 mois)
--      ALTER TABLE oltp.transactions DETACH PARTITION oltp.transactions_2025_09 CONCURRENTLY;
--    puis export de la partition détachée (Parquet, stockage objet) et suppression.
--    L'historique complet reste dans Snowflake (10 ans, Code de commerce).
--
-- 4. Répartition horizontale (sharding), déclenchée à 70 % de la capacité
--    de 5 000 tx/s, soit vers 13 mois (livrable 0 §4.2) : extension Citus
--    (PostgreSQL distribué, même SQL, même Debezium).
--      SELECT create_reference_table('oltp.countries');        -- idem currencies,
--      SELECT create_reference_table('oltp.payment_methods');  -- exchange_rates : copiées partout
--      SELECT create_distributed_table('oltp.merchants', 'merchant_id');
--      SELECT create_distributed_table('oltp.transactions', 'merchant_id',
--                                      colocate_with => 'oltp.merchants');
--      SELECT create_distributed_table('oltp.customers', 'customer_id');
--    Conséquences assumées :
--    * la clé primaire devient (merchant_id, transaction_id, created_at) ;
--    * la clé étrangère transactions -> customers ne peut plus être vérifiée
--      par la base (deux répartitions différentes) : contrôlée par l'API et
--      par un test quotidien de réconciliation ;
--    * transaction_id est généré sans séquence centrale : identifiant 64 bits
--      horodaté (type « Snowflake ID »), qui reste un BIGINT pour RAW,
--      l'étoile et MongoDB.
-- ============================================================
