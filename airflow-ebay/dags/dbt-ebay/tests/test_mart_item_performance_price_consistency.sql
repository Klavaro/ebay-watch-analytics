select *
from {{ ref('mart_item_performance') }}
where price_value < 0
