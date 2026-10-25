-- Quarantaine des règles métier : transactions arrivées dans RAW qui ne peuvent pas entrer dans
-- l'étoile. Au lieu de disparaître en silence (jointure qui échoue) ou de bloquer tout le
-- pipeline (test en échec), elles sont isolées ici avec leur raison.
-- Reconstruite à chaque passage : une transaction dont le client arrive un peu plus tard
-- (dimension arrivée en retard) sort d'elle-même de la quarantaine au passage suivant.

with tx as (
    select * from {{ ref('stg_transactions') }}
),

checked as (
    select
        tx.transaction_id,
        tx.created_at,
        tx._loaded_at,
        case
            when tx.amount is null or tx.amount <= 0 then 'montant absent ou non positif'
            when tx.amount_usd is null              then 'taux de change manquant pour ' || tx.currency_code
            when c.customer_id is null              then 'client inconnu'
            when m.merchant_id is null              then 'commerçant inconnu'
        end as reason
    from tx
    left join {{ source('raw', 'customers') }} c on c.customer_id = tx.customer_id
    left join {{ source('raw', 'merchants') }} m on m.merchant_id = tx.merchant_id
)

select transaction_id, created_at, _loaded_at, reason
from checked
where reason is not null
