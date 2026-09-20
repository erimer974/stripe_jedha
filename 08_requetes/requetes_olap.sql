-- ============================================================
-- Livrable 8 — Requêtes analytiques sur l'étoile (couche ANALYTICS)
-- Questions métier : revenus, fraude, segmentation, performance, conformité.
--
-- SQL portable : s'exécute tel quel sur Snowflake (cible) et sur l'entrepôt
-- PostgreSQL de la maquette (identifiants non quotés, fonctions standard).
-- Rôle : data_analyst (Q1 à Q13), auditor (Q14).
-- ============================================================

-- Q1 — Revenus mensuels par commerçant (analyse des revenus)
SELECT d.year, d.month, m.name AS merchant, SUM(f.amount_usd) AS revenue_usd
FROM analytics.fact_transactions f
JOIN analytics.dim_date     d ON d.date_key = f.date_key
JOIN analytics.dim_merchant m ON m.merchant_key = f.merchant_key
WHERE f.status = 'succeeded'
GROUP BY d.year, d.month, m.name
ORDER BY revenue_usd DESC;

-- Q2 — Taux de transactions signalées frauduleuses par segment client (fraude × segmentation)
SELECT c.segment,
       COUNT(*)                                                AS nb_transactions_scorees,
       ROUND(AVG(CASE WHEN f.is_fraud THEN 1.0 ELSE 0 END), 4) AS taux_fraude
FROM analytics.fact_transactions f
JOIN analytics.dim_customer c ON c.customer_key = f.customer_key
WHERE f.fraud_score IS NOT NULL
GROUP BY c.segment
ORDER BY taux_fraude DESC;

-- Q3 — Revenus quotidiens lus dans le pré-agrégat (sans balayer la table de faits)
SELECT d.full_date,
       SUM(a.total_revenue_usd) AS revenue_usd,
       SUM(a.nb_transactions)   AS nb_transactions,
       SUM(a.nb_flagged_fraud)  AS nb_flagged_fraud
FROM analytics.agg_revenue_daily a
JOIN analytics.dim_date d ON d.date_key = a.date_key
GROUP BY d.full_date
ORDER BY d.full_date DESC;

-- Q4 — Commerçants les plus exposés à la fraude
SELECT m.name AS merchant, m.country,
       SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS nb_flagged,
       ROUND(AVG(f.fraud_score), 4)                AS score_moyen
FROM analytics.fact_transactions f
JOIN analytics.dim_merchant m ON m.merchant_key = f.merchant_key
WHERE f.fraud_score IS NOT NULL
GROUP BY m.name, m.country
ORDER BY nb_flagged DESC
LIMIT 10;

-- Q5 — Couverture du scoring : mesure de la cohérence à terme entre l'étoile et le modèle
SELECT COUNT(*)                      AS transactions,
       COUNT(fraud_score)            AS scorees,
       COUNT(*) - COUNT(fraud_score) AS en_attente_de_score,
       MAX(created_at)               AS derniere_transaction
FROM analytics.fact_transactions;

-- Q9 — Revenus et volume par pays du commerçant (analyse géographique)
SELECT m.country,
       COUNT(*)                                                         AS nb_transactions,
       SUM(CASE WHEN f.status = 'succeeded' THEN f.amount_usd ELSE 0 END) AS revenue_usd
FROM analytics.fact_transactions f
JOIN analytics.dim_merchant m ON m.merchant_key = f.merchant_key
GROUP BY m.country
ORDER BY revenue_usd DESC;

-- Q10 — Série temporelle : revenu quotidien, moyenne mobile sur 7 jours, variation par rapport à la veille
--       La table de dates continue garantit une ligne par jour, même sans transaction.
WITH daily AS (
    SELECT d.full_date, COALESCE(SUM(a.total_revenue_usd), 0) AS revenue_usd
    FROM analytics.dim_date d
    LEFT JOIN analytics.agg_revenue_daily a ON a.date_key = d.date_key
    GROUP BY d.full_date
)
SELECT full_date,
       revenue_usd,
       ROUND(AVG(revenue_usd) OVER (ORDER BY full_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS moyenne_mobile_7j,
       revenue_usd - LAG(revenue_usd) OVER (ORDER BY full_date)                                     AS variation_veille
FROM daily
ORDER BY full_date DESC;

-- Q11 — Montant moyen des transactions signalées frauduleuses et des transactions légitimes
SELECT f.is_fraud,
       COUNT(*)                     AS nb,
       ROUND(AVG(f.amount_usd), 2)  AS montant_moyen_usd
FROM analytics.fact_transactions f
WHERE f.fraud_score IS NOT NULL
GROUP BY f.is_fraud;

-- Q12 — Performance des moyens de paiement : taux de réussite, remboursements, revenus
SELECT p.label AS moyen_de_paiement,
       COUNT(*)                                                           AS nb_transactions,
       ROUND(AVG(CASE WHEN f.status = 'succeeded' THEN 1.0 ELSE 0 END), 4) AS taux_reussite,
       ROUND(AVG(CASE WHEN f.status = 'refunded'  THEN 1.0 ELSE 0 END), 4) AS taux_remboursement,
       SUM(CASE WHEN f.status = 'succeeded' THEN f.amount_usd ELSE 0 END)   AS revenue_usd
FROM analytics.fact_transactions f
JOIN analytics.dim_payment_method p ON p.payment_method_key = f.payment_method_key
GROUP BY p.label
ORDER BY nb_transactions DESC;

-- Q13 — Segmentation client : poids de chaque segment, par pays
SELECT c.segment, c.country,
       COUNT(DISTINCT c.customer_key)                                      AS nb_clients,
       SUM(CASE WHEN f.status = 'succeeded' THEN f.amount_usd ELSE 0 END)   AS revenue_usd
FROM analytics.dim_customer c
LEFT JOIN analytics.fact_transactions f ON f.customer_key = c.customer_key
GROUP BY c.segment, c.country
ORDER BY c.segment, revenue_usd DESC;

-- Q14 — Suivi de conformité sur 30 jours : exécutions et échecs par contrôle (rôle auditor)
SELECT system, check_name, blocking,
       COUNT(*)                                        AS executions,
       SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS echecs,
       MAX(checked_at)                                 AS dernier_controle
FROM audit.compliance_reports
WHERE checked_at >= CURRENT_DATE - 30
GROUP BY system, check_name, blocking
ORDER BY blocking DESC, echecs DESC, system, check_name;
