select distinct
    {{ dbt_utils.generate_surrogate_key(['condition_id']) }} as condition_sk,
    condition_id,
    condition
from {{ ref('stg_ebay_items') }}