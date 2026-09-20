select
    payment_method_id as payment_method_key,
    label
from {{ source('raw', 'payment_methods') }}
