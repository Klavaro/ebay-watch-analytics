select
    item_id,
    opt.value:shippingCostType::string as shipping_cost_type,
    opt.value:shippingCost:value::float as shipping_cost,
    opt.value:shippingCost:currency::string as shipping_currency,
    opt.value:shipToLocations::string as ship_to_locations
from {{ ref('stg_ebay_items') }},
     lateral flatten(input => shipping_options) as opt
