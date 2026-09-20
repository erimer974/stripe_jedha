select
    row_number() over (order by m.merchant_id) as merchant_key,
    m.merchant_id,
    m.name,
    co.name                                    as country
from {{ source('raw', 'merchants') }} m
left join {{ source('raw', 'countries') }} co on co.country_code = m.country_code
