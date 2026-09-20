-- Dimension client, dénormalisée (pays en clair) et enrichie d'un segment calculé
-- sur le montant cumulé en USD : enrichissement analytique propre à l'OLAP.
-- Aucune donnée personnelle directe : l'email n'existe pas dans l'entrepôt.

with totals as (
    select customer_id, sum(amount_usd) as total_usd
    from {{ ref('stg_transactions') }}
    where status = 'succeeded'
    group by customer_id
)

select
    row_number() over (order by c.customer_id) as customer_key,
    c.customer_id,
    co.name                                    as country,
    case
        when coalesce(t.total_usd, 0) >= 5000 then 'high_value'
        when coalesce(t.total_usd, 0) >= 500  then 'regular'
        else 'occasional'
    end                                        as segment
from {{ source('raw', 'customers') }} c
left join {{ source('raw', 'countries') }} co on co.country_code = c.country_code
left join totals t on t.customer_id = c.customer_id
