-- Réconciliation par transaction_id : aucune transaction chargée dans RAW ne doit
-- manquer dans l'étoile (une jointure de dimension qui échoue ferait perdre des lignes).
-- Le test échoue s'il renvoie au moins une ligne.

select r.transaction_id
from {{ source('raw', 'transactions') }} r
left join {{ ref('fact_transactions') }} f on f.transaction_id = r.transaction_id
where f.transaction_id is null
