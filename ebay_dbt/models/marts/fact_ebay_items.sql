{{ config(materialized='incremental', schema='marts') }}

with staged as (
    select * from {{ ref('stg_ebay_items') }}
),

joined as (
    select
        -- surrogate keys
        {{ dbt_utils.generate_surrogate_key(['condition', 'condition_id']) }} as condition_sk,
        {{ dbt_utils.generate_surrogate_key(['price_value', 'price_currency']) }} as price_sk,
        {{ dbt_utils.generate_surrogate_key(['seller_username', 'seller_feedback_percentage', 'seller_feedback_score']) }} as seller_sk,
        {{ dbt_utils.generate_surrogate_key(['item_location_country']) }} as location_sk,

        -- facts
        item_id,
        title,
        item_href,
        item_web_url,
        thumbnail_images,
        shipping_options,
        additional_images,
        adult_only,
        legacy_item_id,
        available_coupons,
        item_origin_date,
        item_creation_date,
        top_rated_buying_experience,
        priority_listing,
        listing_marketplace_id,
        image_url,
        buying_options,
        load_timestamp
    from staged
)

select * from joined
