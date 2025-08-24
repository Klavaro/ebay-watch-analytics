{{ config(materialized='table') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['item_id']) }} as item_listing_sk,
    {{ dbt_utils.generate_surrogate_key(['opt.value::string']) }} as buying_option_sk
from {{ ref('stg_ebay_items') }},
     lateral flatten(input => buying_options) as opt
