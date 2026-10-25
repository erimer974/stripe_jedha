-- Réconciliation par transaction_id : toute transaction chargée dans RAW doit se retrouver
-- soit dans l'étoile, soit en quarantaine avec sa raison. Aucune ne doit disparaître en silence.
-- Le test échoue s'il renvoie au moins une ligne.
--
-- La voie flux alimente RAW en continu, y compris pendant dbt run : on ne contrôle donc que
-- les transactions arrivées AVANT la construction de l'étoile (filigrane = _loaded_at le plus
-- récent de l'étoile), avec une marge d'une minute pour un client ou un commerçant publié
-- sur son topic un peu après la transaction (dimension arrivée en retard).

with watermark as (
    select max(_loaded_at) - interval '1 minute' as checked_until
    from {{ ref('fact_transactions') }}
)

select r.transaction_id
from {{ source('raw', 'transactions') }} r
cross join watermark w
left join {{ ref('fact_transactions') }} f on f.transaction_id = r.transaction_id
left join {{ ref('quarantine_transactions') }} q on q.transaction_id = r.transaction_id
where f.transaction_id is null
  and q.transaction_id is null
  and r._loaded_at <= w.checked_until
