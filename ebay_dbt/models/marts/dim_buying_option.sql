{{ config(materialized='incremental') }}

with source as (
    select item_id, buying_options
    from {{ ref('stg_ebay_items') }}
    where buying_options is not null
),

flattened as (
    select
        item_id,
        value::string as buying_option
    from source,
    lateral flatten(input => try_parse_json(buying_options))
),

unique_options as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['buying_option']) }} as buying_option_sk,
        buying_option
    from flattened
)

select * from unique_options
