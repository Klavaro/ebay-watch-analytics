{% macro category_price_bands(price_field) %}
    approx_percentile({{ price_field }}, 0.25) as price_p25,
    approx_percentile({{ price_field }}, 0.50) as price_median,
    approx_percentile({{ price_field }}, 0.75) as price_p75,
    avg({{ price_field }}) as avg_price,
    stddev({{ price_field }}) as price_stddev
{% endmacro %}
