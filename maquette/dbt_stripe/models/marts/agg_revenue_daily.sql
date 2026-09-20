-- Pré-agrégat : revenus et fraude quotidiens par commerçant (Livrable 3).
-- Reconstruit à chaque passage du pipeline, lu directement par les tableaux de bord.

select
    date_key,
    merchant_key,
    sum(case when status = 'succeeded' then amount_usd else 0 end) as total_revenue_usd,
    count(*)                                                       as nb_transactions,
    count(fraud_score)                                             as nb_scored,
    sum(case when is_fraud then 1 else 0 end)                      as nb_flagged_fraud
from {{ ref('fact_transactions') }}
group by date_key, merchant_key
