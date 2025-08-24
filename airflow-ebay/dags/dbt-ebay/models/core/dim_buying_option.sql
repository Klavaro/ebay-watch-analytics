{{ config(materialized='table') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['(opt.value)::string']) }} as buying_option_sk,
    opt.value::string as buying_option
from {{ ref('stg_ebay_items') }},
     lateral flatten(input => buying_options) as opt
