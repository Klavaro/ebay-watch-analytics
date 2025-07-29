select distinct
    {{ dbt_utils.generate_surrogate_key(['category_id']) }} as category_sk,
    category_id,
    category_name
from {{ ref('stg_ebay_items') }}