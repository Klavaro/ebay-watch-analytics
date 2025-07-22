{{ config(materialized='incremental') }}

with source as (
    select item_id, shipping_options
    from {{ ref('stg_ebay_items') }}
    where shipping_options is not null
),

flattened as (
    select
        item_id,
        shipping.value:shippingCostType::string as shipping_cost_type,
        shipping.value:shippingCost.currency::string as shipping_currency,
        shipping.value:shippingCost.value::float as shipping_value
    from source,
    lateral flatten(input => try_parse_json(shipping_options)) as shipping
),

deduped as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['shipping_cost_type', 'shipping_currency', 'shipping_value']) }} as shipping_option_sk,
        shipping_cost_type,
        shipping_currency,
        shipping_value
    from flattened
)

select * from deduped
