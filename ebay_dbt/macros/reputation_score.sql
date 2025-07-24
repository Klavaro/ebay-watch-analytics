{% macro reputation_score(score, percentage) %}
    (
        {{ score }} * 0.6 +
        {{ percentage }} * 0.4
    ) / 100
{% endmacro %}
