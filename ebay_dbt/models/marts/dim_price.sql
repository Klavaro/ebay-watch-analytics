{{ config(materialized='incremental') }}

with source as (
    select distinct
        price_value,
        price_currency
    from {{ ref('stg_ebay_items') }}
),

deduplicated as (
    select
        {{ dbt_utils.generate_surrogate_key(['price_value', 'price_currency']) }} as price_sk,
        price_value,
        price_currency
    from source
)

select * from deduplicated
