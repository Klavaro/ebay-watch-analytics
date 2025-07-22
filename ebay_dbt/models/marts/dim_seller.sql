{{ config(materialized='incremental') }}

with source as (
    select distinct
        seller_username,
        seller_feedback_percentage,
        seller_feedback_score
    from {{ ref('stg_ebay_items') }}
),

deduplicated as (
    select
        {{ dbt_utils.generate_surrogate_key(['seller_username', 'seller_feedback_percentage', 'seller_feedback_score']) }} as seller_sk,
        seller_username,
        seller_feedback_percentage,
        seller_feedback_score
    from source
)

select * from deduplicated
