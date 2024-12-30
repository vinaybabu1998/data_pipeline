{% macro common_aggregations(table_name, column_name, time_period) %}
WITH aggregated_data AS (
    SELECT
        DATE_TRUNC(order_date, {{ time_period }}) AS period, -- Dynamically adjust time period
        SUM({{ column_name }}) AS total_value,
        AVG({{ column_name }}) AS average_value,
        ARRAY_AGG({{ column_name }} ORDER BY {{ column_name }} LIMIT 2)[SAFE_OFFSET(1)] AS median_value,
        '{{ column_name }}' AS column_name
    FROM {{ table_name }}
    GROUP BY period
)
SELECT
    period,
    total_value,
    average_value,
    median_value,
    column_name
FROM aggregated_data
ORDER BY period
{% endmacro %}
