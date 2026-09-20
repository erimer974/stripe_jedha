-- Staging : prépare les transactions brutes pour l'étoile.
--   * conversion en USD au taux de référence en vigueur à la date de la transaction ;
--   * rattachement du score de fraude (ml.fraud_scores). Une transaction scorée après
--     ce run reçoit son score au passage suivant (5 min) : cohérence éventuelle assumée.

with tx as (
    select * from {{ source('raw', 'transactions') }}
),

scores as (
    select * from {{ source('ml', 'fraud_scores') }}
)

select
    tx.transaction_id,
    tx.customer_id,
    tx.merchant_id,
    tx.payment_method_id,
    tx.currency_code,
    tx.amount,
    round(tx.amount * rate.rate_to_usd, 2)               as amount_usd,
    tx.device_type,
    tx.status,
    scores.fraud_score,
    scores.model_version,
    case when scores.fraud_score is null then null
         else scores.fraud_score >= 0.5 end              as is_fraud,
    tx.created_at,
    tx.created_at::date                                  as transaction_date
from tx
left join lateral (
    select r.rate_to_usd
    from {{ source('raw', 'exchange_rates') }} r
    where r.currency_code = tx.currency_code
      and r.rate_date <= tx.created_at::date
    order by r.rate_date desc
    limit 1
) rate on true
left join scores on scores.transaction_id = tx.transaction_id
