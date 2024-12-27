{% macro common_aggregations(table_name, column_name, time_period='month') %}
    -- This macro calculates total, average, and median for a given numeric column
    -- and allows the user to specify the time period for aggregation.
    
    -- Set default time period to 'month' if none is provided
    {%- set time_period = time_period | default('month') -%}
    
    WITH aggregated_data AS (
        SELECT
            {% if time_period == 'month' %}
                DATE_TRUNC(order_date, MONTH) AS period
            {% elif time_period == 'quarter' %}
                DATE_TRUNC(order_date, QUARTER) AS period
            {% elif time_period == 'year' %}
                DATE_TRUNC(order_date, YEAR) AS period
            {% else %}
                DATE_TRUNC(order_date, MONTH) AS period  -- Default case
            {% endif %},
            SUM({{ column_name }}) AS total_value,
            AVG({{ column_name }}) AS average_value,
            ARRAY_AGG({{ column_name }} ORDER BY {{ column_name }} LIMIT 2)[SAFE_OFFSET(1)] AS median_value
        FROM {{ table_name }}
        GROUP BY period
    )

    SELECT
        period,
        total_value,
        average_value,
        median_value,
        '{{ column_name }}' AS column_name  -- Add the column name in the output for reference
    FROM aggregated_data
    ORDER BY period

{% endmacro %}
