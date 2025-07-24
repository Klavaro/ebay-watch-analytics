-- Purpose: Evaluate category-level pricing distribution to find investment opportunities or underpriced segments.
-- Business Questions:
--  What’s the typical price band for items in each category?
--  Are some categories more price-volatile than others?

with base as (
    select
        category_sk,
        {{ ref('fact_item_listing') }}.price_value as price
    from {{ ref('fact_item_listing') }}
),

bands as (
    select
        category_sk,
        {{ category_price_bands('price') }}
    from base
    group by category_sk
)

select * from bands
