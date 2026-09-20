#!/bin/bash
# ============================================================
# SOURCE OLTP — sécurité & conformité (sous-ensemble exécutable du Livrable 6)
#   1. RBAC : un rôle par usage, au moindre privilège (jusqu'à la colonne)
#   2. Journalisation d'audit
#   3. RGPD : pseudonymisation + procédure d'effacement
#   4. Vues de contrôle lues par le DAG stripe_compliance_daily
# Non reproduit dans la maquette (documenté comme politique cible) :
#   TLS (certificat à monter), chiffrement au repos, sauvegardes / reprise après sinistre.
# ============================================================
set -e

psql -v ON_ERROR_STOP=1 -v svc_pw="$SERVICE_PASSWORD" \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'EOSQL'

-- ---- 1. RBAC ----
CREATE ROLE payment_service LOGIN PASSWORD :'svc_pw';  -- l'application de paiement
CREATE ROLE el_reader       LOGIN PASSWORD :'svc_pw';  -- le connecteur d'ingestion (rôle d'Airbyte), lecture seule
CREATE ROLE fraud_writer    LOGIN PASSWORD :'svc_pw';  -- le scoring : écrit uniquement la colonne fraud_score
CREATE ROLE data_analyst    LOGIN PASSWORD :'svc_pw';  -- l'analyste opérationnel
CREATE ROLE auditor         LOGIN PASSWORD :'svc_pw';  -- l'auditeur conformité

REVOKE CONNECT ON DATABASE stripe_oltp FROM PUBLIC;
GRANT CONNECT ON DATABASE stripe_oltp
    TO payment_service, el_reader, fraud_writer, data_analyst, auditor;
-- auditor : USAGE seul (nommer les tables dans les contrôles de droits), aucun droit sur les données
GRANT USAGE ON SCHEMA oltp TO payment_service, el_reader, fraud_writer, data_analyst, auditor;

SET search_path TO oltp;

-- Application : lit la référence, crée clients / commerçants / transactions
GRANT SELECT ON countries, currencies, exchange_rates, payment_methods TO payment_service;
GRANT SELECT, INSERT ON customers, merchants, transactions TO payment_service;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA oltp TO payment_service;

-- Ingestion : lecture seule, et JAMAIS l'email (minimisation RGPD dès l'extraction)
GRANT SELECT ON countries, currencies, exchange_rates, payment_methods, merchants, transactions
    TO el_reader;
GRANT SELECT (customer_id, country_code, created_at) ON customers TO el_reader;

-- Scoring : ne voit que l'identifiant et le score, ne modifie que le score
GRANT SELECT (transaction_id, fraud_score) ON transactions TO fraud_writer;
GRANT UPDATE (fraud_score) ON transactions TO fraud_writer;

-- ---- 3. RGPD : pseudonymisation pour l'analyste ----
CREATE VIEW customers_pseudo AS
SELECT customer_id, md5(email) AS email_hash, country_code, created_at
FROM customers;

GRANT SELECT ON transactions, merchants, countries, currencies, payment_methods, customers_pseudo
    TO data_analyst;

-- Droit à l'effacement : anonymisation irréversible, en conservant l'historique transactionnel
CREATE FUNCTION erase_customer(p_customer_id BIGINT) RETURNS void
LANGUAGE sql SECURITY DEFINER SET search_path = oltp AS $$
    UPDATE customers
    SET email = 'erased_' || customer_id || '@anon.invalid'
    WHERE customer_id = p_customer_id;
$$;
REVOKE ALL ON FUNCTION erase_customer(BIGINT) FROM PUBLIC;

-- ---- 2. Journalisation d'audit ----
ALTER SYSTEM SET log_connections    = 'on';
ALTER SYSTEM SET log_disconnections = 'on';
ALTER SYSTEM SET log_statement      = 'ddl';
SELECT pg_reload_conf();

-- ---- 4. Vues de contrôle (schéma audit, lecture réservée à l'auditeur) ----
-- Elles lisent le catalogue système (pg_catalog), visible quel que soit le rôle,
-- et non information_schema, qui masque les droits des autres rôles.
CREATE SCHEMA audit;
GRANT USAGE ON SCHEMA audit TO auditor;
GRANT pg_read_all_settings TO auditor;

-- Revue des habilitations : droits sur tables ET sur colonnes
CREATE VIEW audit.grants_report AS
SELECT r.rolname AS grantee, c.relname AS object_name, a.privilege_type
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL aclexplode(c.relacl) a
JOIN pg_roles r ON r.oid = a.grantee
WHERE n.nspname = 'oltp'
UNION ALL
SELECT r.rolname, c.relname || '.' || att.attname, a.privilege_type
FROM pg_attribute att
JOIN pg_class c ON c.oid = att.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL aclexplode(att.attacl) a
JOIN pg_roles r ON r.oid = a.grantee
WHERE n.nspname = 'oltp';

-- Contrôle bloquant : aucun rôle de connexion, hors application de paiement, ne lit l'email en clair
CREATE VIEW audit.pii_access_violations AS
SELECT r.rolname AS role_name, 'oltp.customers.email' AS pii_column
FROM pg_roles r
WHERE r.rolcanlogin
  AND NOT r.rolsuper
  AND r.rolname <> 'payment_service'
  AND has_column_privilege(r.rolname, 'oltp.customers', 'email', 'SELECT');

-- Contrôle bloquant PCI-DSS : aucune colonne de données de carte
CREATE VIEW audit.pci_card_data_columns AS
SELECT c.relname AS table_name, att.attname AS column_name
FROM pg_attribute att
JOIN pg_class c ON c.oid = att.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'oltp' AND att.attnum > 0 AND NOT att.attisdropped
  AND att.attname ~* '(card_number|^pan$|cvv|cvc)';

-- Suivi du droit à l'oubli
CREATE VIEW audit.erasure_report AS
SELECT count(*) AS erased_customers
FROM oltp.customers
WHERE email LIKE 'erased\_%@anon.invalid';

-- Paramètres d'audit et de chiffrement en transit
CREATE VIEW audit.security_settings AS
SELECT name, setting
FROM pg_settings
WHERE name IN ('log_connections', 'log_disconnections', 'log_statement', 'ssl');

GRANT SELECT ON ALL TABLES IN SCHEMA audit TO auditor;
EOSQL
