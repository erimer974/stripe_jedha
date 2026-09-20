-- Dimension date (date spine) bornée sur la période couverte par les transactions.

with bounds as (
    select
        coalesce(min(transaction_date), current_date) as min_date,
        coalesce(max(transaction_date), current_date) as max_date
    from {{ ref('stg_transactions') }}
),

spine as (
    select generate_series(
        (select min_date from bounds),
        (select max_date from bounds),
        interval '1 day'
    )::date as full_date
)

select
    to_char(full_date, 'YYYYMMDD')::int  as date_key,
    full_date,
    extract(year from full_date)::int    as year,
    extract(quarter from full_date)::int as quarter,
    extract(month from full_date)::int   as month,
    extract(day from full_date)::int     as day,
    extract(isodow from full_date)::int  as day_of_week
from spine
