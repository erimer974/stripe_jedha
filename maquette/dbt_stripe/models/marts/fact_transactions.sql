-- Table de faits, grain = 1 transaction (Livrable 3).
-- transaction_id = dimension dégénérée : clé de réconciliation OLTP / OLAP / NoSQL.

select
    row_number() over (order by t.transaction_id) as transaction_key,
    d.date_key,
    dc.customer_key,
    dm.merchant_key,
    t.payment_method_id                           as payment_method_key,
    t.transaction_id,
    t.currency_code,
    t.amount_usd,
    t.status,
    t.device_type,
    t.fraud_score,
    t.is_fraud,
    t.model_version,
    t.created_at
from {{ ref('stg_transactions') }} t
join {{ ref('dim_date') }}     d  on d.full_date    = t.transaction_date
join {{ ref('dim_customer') }} dc on dc.customer_id = t.customer_id
join {{ ref('dim_merchant') }} dm on dm.merchant_id = t.merchant_id
