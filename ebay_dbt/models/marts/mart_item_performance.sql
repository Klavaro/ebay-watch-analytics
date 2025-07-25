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
        s.shipping_cost,
        count(distinct i.image_url) as image_count,
        c.condition_id
    from {{ ref('fact_item_listing') }} f
    left join {{ ref('bridge_item_shipping_option') }} s using (item_id)
    left join {{ ref('bridge_item_image') }} i using (item_id)
    left join {{ ref('dim_condition') }} c using (condition_sk)
    group by 1, 2, 3, 4, 5, 6, 7, 8, 10
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
