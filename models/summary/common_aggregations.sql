-- macros/common_aggregations.sql

{% macro common_aggregations(clean_sales, amount) %}
WITH aggregated_data AS (
    SELECT
        SUM({{ amount }}) AS total_value,
        AVG({{ amount }}) AS average_value,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {{ amount }}) AS median_value
    FROM `data-pipeline-project-445905`.`dbt_vkv`.`clean_sales`
)
SELECT
    total_value,
    average_value,
    median_value
FROM aggregated_data
{% endmacro %}
