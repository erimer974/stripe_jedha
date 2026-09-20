-- ============================================================
-- Requêtes opérationnelles sur l'OLTP — requêtes Q6 à Q8 du livrable 8
-- Copie exécutable de 08_requetes/requetes_oltp.sql.
-- À exécuter avec le rôle data_analyst sur la base OLTP :
--   docker compose exec -T oltp-db psql "postgresql://data_analyst:<SERVICE_PASSWORD>@localhost/stripe_oltp" < queries/02_oltp_operations.sql
-- ============================================================

-- Q6 — Transactions échouées de la dernière heure (supervision opérationnelle)
SELECT transaction_id, amount, currency_code, status, created_at
FROM oltp.transactions
WHERE status = 'failed'
  AND created_at > now() - INTERVAL '1 hour'
ORDER BY created_at DESC
LIMIT 20;

-- Q7 — Transactions récentes à haut risque : file de revue des analystes fraude
--      (score réécrit par le modèle ; index partiel et index sur created_at)
SELECT transaction_id, customer_id, amount, currency_code, fraud_score, created_at
FROM oltp.transactions
WHERE fraud_score >= 0.9
ORDER BY created_at DESC
LIMIT 20;

-- Q8 — Meilleurs clients sur 30 jours, identité pseudonymisée (RGPD)
SELECT p.email_hash, COUNT(*) AS nb, SUM(t.amount) AS total
FROM oltp.transactions t
JOIN oltp.customers_pseudo p ON p.customer_id = t.customer_id
WHERE t.created_at > now() - INTERVAL '30 days'
  AND t.status = 'succeeded'
GROUP BY p.email_hash
ORDER BY total DESC
LIMIT 10;
