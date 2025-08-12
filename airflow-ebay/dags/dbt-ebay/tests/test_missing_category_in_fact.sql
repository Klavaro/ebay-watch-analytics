select f.item_id
from {{ ref('fact_item_listing') }} f
left join {{ ref('dim_category') }} c
  on f.category_sk = c.category_sk
where c.category_sk is null
