-- models/dimensions/dim_date.sql

with dates as (
    select distinct cast(item_origin_date as date) as date_val
    from {{ ref('stg_ebay_items') }}
    union
    select distinct cast(item_creation_date as date) as date_val
    from {{ ref('stg_ebay_items') }}
    union
    select distinct cast(load_timestamp as date) as date_val
    from {{ ref('stg_ebay_items') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['cast(date_val as date)']) }} as date_sk,
    date_val as date,
    extract(year from date_val) as year,
    extract(month from date_val) as month,
    extract(day from date_val) as day,
    extract(quarter from date_val) as quarter,
    to_char(date_val, 'Day') as day_of_week
from dates
