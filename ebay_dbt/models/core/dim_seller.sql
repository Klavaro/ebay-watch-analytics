select distinct
    {{ dbt_utils.generate_surrogate_key(['seller_username']) }} as seller_sk,
    seller_username,
    seller_feedback_score,
    seller_feedback_percentage
from {{ ref('stg_ebay_items') }}