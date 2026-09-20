-- Règles métier : montant converti strictement positif, score de fraude compris entre 0 et 1.

select transaction_id, amount_usd, fraud_score
from {{ ref('fact_transactions') }}
where amount_usd <= 0
   or (fraud_score is not null and (fraud_score < 0 or fraud_score > 1))
