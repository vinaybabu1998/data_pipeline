{% macro common_aggregations(table_name, date_column, numeric_column, period) %}
WITH aggregated_data AS (
    SELECT
        DATE_TRUNC({{ date_column }}, {{ period | upper }}) AS period,  -- Dynamically adjust time period
        SUM({{ numeric_column }}) AS total_value,  -- Sum of the numeric column
        AVG({{ numeric_column }}) AS average_value,  -- Average of the numeric column
        ARRAY_AGG({{ numeric_column }} ORDER BY {{ numeric_column }} LIMIT 2)[SAFE_OFFSET(1)] AS median_value,  -- Median calculation
        '{{ numeric_column }}' AS column_name  -- Column name for identification
    FROM {{ table_name }}  -- Table to aggregate from
    GROUP BY period  -- Grouping by the defined time period
)

-- Returning aggregated results
SELECT
    period,
    total_value,
    average_value,
    median_value,
    column_name
FROM aggregated_data
ORDER BY period  -- Ordering results by the time period
{% endmacro %}
