select
    item_id,
    opt.value as buying_option
from {{ ref('stg_ebay_items') }},
     lateral flatten(input => buying_options) as opt
