{{ config(materialized='incremental') }}

with source as (
    select distinct
        condition,
        condition_id
    from {{ ref('stg_ebay_items') }}
),

deduplicated as (
    select
        {{ dbt_utils.generate_surrogate_key(['condition', 'condition_id']) }} as condition_sk,
        condition,
        condition_id
    from source
)

select * from deduplicated
