-- ÉCHEC si plus de 1 % des transactions du dernier jour sont en quarantaine : le problème est
-- massif (référentiel cassé, source défaillante). La voie lots s'arrête avant le rattrapage du
-- modèle et l'alerte part (on_failure_callback).

with day_total as (
    select count(*) as n from {{ ref('stg_transactions') }} where created_at > now() - interval '1 day'
),
day_quarantined as (
    select count(*) as n from {{ ref('quarantine_transactions') }} where created_at > now() - interval '1 day'
)

select q.n as quarantined, t.n as total
from day_quarantined q, day_total t
where t.n > 0 and q.n > 0.01 * t.n
