{{ config(materialized='incremental') }}

with source as (
    select item_id, thumbnail_images
    from {{ ref('stg_ebay_items') }}
    where thumbnail_images is not null
),

flattened as (
    select
        item_id,
        img.value:url::string as image_url
    from source,
    lateral flatten(input => try_parse_json(thumbnail_images)) as img
),

deduped as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['image_url']) }} as thumbnail_sk,
        image_url
    from flattened
)

select * from deduped
