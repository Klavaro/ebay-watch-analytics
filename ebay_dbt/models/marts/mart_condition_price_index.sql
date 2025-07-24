-- Purpose: Compare pricing trends across item conditions to identify gaps or arbitrage.
-- Business Questions:
--  How much more do "New" items sell for compared to "Used"?
--  Is the "Refurbished" segment underpriced?

with subquery as (
    SELECT
      AVG(price_value) AS avg_all_price,
      STDDEV(price_value) AS stddev_all_price
    FROM {{ ref('fact_item_listing') }}
)
SELECT
  condition_sk,
  COUNT(*) AS item_count,
  MIN(price_value) AS min_price,
  MAX(price_value) AS max_price,
  AVG(price_value) AS avg_price,
  STDDEV(price_value) AS stddev_price,
  -- Normalized score: z-score relative to all items
  (AVG(price_value) - subquery.avg_all_price) / NULLIF(subquery.stddev_all_price, 0) AS normalized_price_score
FROM {{ ref('fact_item_listing') }}, subquery
GROUP BY condition_sk, subquery.avg_all_price, subquery.stddev_all_price
