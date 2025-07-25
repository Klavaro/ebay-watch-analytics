{% macro listing_quality_score(
    top_rated,
    available_coupons,
    priority_listing,
    shipping_cost,
    condition_id,
    image_count
) %}
    (
        case when {{ top_rated }} then 1 else 0 end +
        case when {{ available_coupons }} then 1 else 0 end +
        case when {{ priority_listing }} then 1 else 0 end +
        case when {{ shipping_cost }} is not null and {{ shipping_cost }} <= 5 then 1 else 0 end +
        case when {{ condition_id }} in (1000, 1500) then 1 else 0 end + -- New/Like New
        case when {{ image_count }} >= 3 then 1 else 0 end
    ) as listing_quality_score
{% endmacro %}
