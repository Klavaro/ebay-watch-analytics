{{ config(materialized='table') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['img.value:imageUrl::string','\'THUMBNAIL\'']) }} as image_sk,
    img.value:imageUrl::string as image_url,
    'THUMBNAIL' as image_type
from {{ ref('stg_ebay_items') }},
     lateral flatten(input => thumbnail_images) as img

union all

select distinct
    {{ dbt_utils.generate_surrogate_key(['img.value:imageUrl::string','\'ADDITIONAL\'']) }} as image_sk,
    img.value:imageUrl::string as image_url,
    'ADDITIONAL' as image_type
from {{ ref('stg_ebay_items') }},
     lateral flatten(input => additional_images) as img
