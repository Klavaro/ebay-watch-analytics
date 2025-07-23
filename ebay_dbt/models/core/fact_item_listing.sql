select
    {{ dbt_utils.generate_surrogate_key(['item_id']) }} as item_listing_sk,
    item_id,
    title,

    -- Foreign keys (surrogate)
    {{ dbt_utils.generate_surrogate_key(['condition_id']) }} as condition_sk,
    {{ dbt_utils.generate_surrogate_key(['category_id']) }} as category_sk,
    {{ dbt_utils.generate_surrogate_key(['seller_username']) }} as seller_sk,
    {{ dbt_utils.generate_surrogate_key(['cast(item_origin_date as date)']) }} as origin_date_sk,
    {{ dbt_utils.generate_surrogate_key(['cast(item_creation_date as date)']) }} as creation_date_sk,
    {{ dbt_utils.generate_surrogate_key(['cast(load_timestamp as date)']) }} as load_date_sk,

    -- Attributes
    listing_marketplace_id as marketplace_id,
    item_location_country,
    price_value,
    price_currency,
    adult_only,
    available_coupons,
    top_rated_buying_experience,
    priority_listing

from {{ ref('stg_ebay_items') }}
