select
    {{ dbt_utils.generate_surrogate_key(['seller_username_cleaned']) }} as seller_sk,
    seller_username_cleaned as seller_username,
    seller_feedback_score,
    seller_feedback_percentage,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    case when dbt_valid_to is null then true else false end as is_current
from {{ ref('snap_seller') }}
where is_current = true

-- dbt run --select staging
-- dbt snapshot
-- dbt run --select core
-- dbt run --select marts