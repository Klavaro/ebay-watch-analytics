select *
from {{ ref('fact_item_listing') }}
where priority_listing not in (true, false)
   or available_coupons not in (true, false)
   or top_rated_buying_experience not in (true, false)
