-- macros/common_aggregations.sql

{% macro common_aggregations(table_name, column_name) %}
WITH aggregated_data AS (
    SELECT
        SUM({{ column_name }}) AS total_value,
        AVG({{ column_name }}) AS average_value,
        ARRAY_AGG({{ column_name }} ORDER BY {{ column_name }} LIMIT 2)[OFFSET(SAFE_OFFSET(1))] AS median_value
    FROM {{ table_name }}
)
SELECT
    total_value,
    average_value,
    median_value
FROM aggregated_data
{% endmacro %}
