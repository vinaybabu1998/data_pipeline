-- models/summary/common_aggregations_model.sql

{{ config(materialized='view') }}

-- Define the macro to handle column, table, and time period dynamically
{% macro common_aggregations(table_name, column_name, time_period='MONTH') %}

WITH aggregated_data AS (
    -- Aggregation for the specified table, column, and time period
    SELECT
        DATE_TRUNC(order_date, {{ time_period }}) AS period,  -- Dynamically use time period (DAY, MONTH, etc.)
        SUM({{ column_name }}) AS total_value,
        AVG({{ column_name }}) AS average_value,
        ARRAY_AGG({{ column_name }} ORDER BY {{ column_name }} LIMIT 2)[SAFE_OFFSET(1)] AS median_value,
        '{{ column_name }}' AS column_name
    FROM `data-pipeline-project-445905.dbt_vkv.{{ table_name }}`
    GROUP BY period
)

-- Final output from aggregated_data
SELECT
    period,
    total_value,
    average_value,
    median_value,
    column_name
FROM aggregated_data
ORDER BY period

{% endmacro %}
