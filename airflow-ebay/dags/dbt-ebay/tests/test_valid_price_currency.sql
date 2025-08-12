select *
from {{ ref('fact_item_listing') }}
where price_currency not in ('USD', 'EUR', 'GBP', 'JPY')
