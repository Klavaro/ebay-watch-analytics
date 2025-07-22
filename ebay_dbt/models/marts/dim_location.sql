{{ config(materialized='incremental') }}

with source as (
    select distinct
        item_location_country
    from {{ ref('stg_ebay_items') }}
),

deduplicated as (
    select
        {{ dbt_utils.generate_surrogate_key(['item_location_country']) }} as location_sk,
        item_location_country
    from source
)

select * from deduplicated
