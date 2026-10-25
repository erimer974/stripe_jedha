-- ============================================================
-- MONTÉE EN CHARGE — partitionnement mensuel de l'OLTP (Livrable 2 §7)
-- À lancer avec le rôle data_analyst (voir README §7).
-- ============================================================

-- 1. Les partitions : une par mois, créées à l'avance ; la partition par défaut reste vide
SELECT c.relname                              AS partition,
       pg_get_expr(c.relpartbound, c.oid)     AS bornes
FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
WHERE i.inhparent = 'oltp.transactions'::regclass
ORDER BY 1;

-- 2. Élagage des partitions : une requête sur octobre 2026 ne lit QUE la partition d'octobre.
--    Les autres mois ne sont même pas ouverts, quelle que soit leur taille.
EXPLAIN (COSTS OFF)
SELECT count(*), sum(amount)
FROM oltp.transactions
WHERE created_at >= '2026-10-01' AND created_at < '2026-11-01';
