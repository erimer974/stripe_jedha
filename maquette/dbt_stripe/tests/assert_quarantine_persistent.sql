-- AVERTISSEMENT : une transaction reste en quarantaine plus de 10 minutes. Elle ne se résorbe
-- pas d'elle-même (ce n'est pas une dimension arrivée en retard) : quelqu'un doit corriger la
-- donnée ou le référentiel. La voie lots continue.
{{ config(severity='warn') }}

select transaction_id, reason
from {{ ref('quarantine_transactions') }}
where _loaded_at < now() - interval '10 minutes'
