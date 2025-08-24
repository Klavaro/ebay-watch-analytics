-- Purpose: Quantify the attractiveness of a listing based on its presentation, shipping, and promotional features. 
-- Business Questions:
--  Which listings are most appealing based on features like condition, shipping, and presentation?
--  How do high-quality listings correlate with pricing or conversion trends?
--  Are certain marketplaces consistently offering better-quality listings?
--  Can we use listing quality to predict inventory turnover or sales success?
with listing as (
    select
        f.item_listing_sk,
        f.title,
        f.condition_sk,
        f.price_value,
        f.priority_listing,
        f.top_rated_buying_experience,
        f.available_coupons,
        
        -- join through bridge → dim for shipping
        min(dso.shipping_cost) as shipping_cost,   -- or avg/max depending on your use case
        
        -- join through bridge → dim for images
        count(distinct di.image_url) as image_count,
        
        c.condition_id
    from {{ ref('fact_item_listing') }} f
    
    -- Shipping bridge + dim
    left join {{ ref('bridge_item_shipping_option') }} bso 
        on f.item_listing_sk = bso.item_listing_sk
    left join {{ ref('dim_shipping_option') }} dso
        on bso.shipping_option_sk = dso.shipping_option_sk
    
    -- Image bridge + dim
    left join {{ ref('bridge_item_image') }} bi
        on f.item_listing_sk = bi.item_listing_sk
    left join {{ ref('dim_image') }} di
        on bi.image_sk = di.image_sk
    
    -- Condition dim (direct, since fact already has condition_sk)
    left join {{ ref('dim_condition') }} c
        on f.condition_sk = c.condition_sk
    
    group by
        f.item_listing_sk,
        f.title,
        f.condition_sk,
        f.price_value,
        f.priority_listing,
        f.top_rated_buying_experience,
        f.available_coupons,
        c.condition_id
),

scored as (
    select *,
        {{ listing_quality_score(
            'top_rated_buying_experience',
            'available_coupons',
            'priority_listing',
            'shipping_cost',
            'condition_id',
            'image_count'
        ) }}
    from listing
)

select * from scored
