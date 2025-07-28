select
    item_id,
    img.value:imageUrl::string as image_url,
    'THUMBNAIL' as image_type
from {{ ref('stg_ebay_items') }},
     lateral flatten(input => thumbnail_images) as img

union all

select
    item_id,
    img.value:imageUrl::string as image_url,
    'ADDITIONAL' as image_type
from {{ ref('stg_ebay_items') }},
     lateral flatten(input => additional_images) as img