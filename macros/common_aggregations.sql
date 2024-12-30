{% macro calculate_aggregations(numeric_column, date_column, start_date, end_date, time_period='monthly') %}
    WITH data AS (
        SELECT
            {{ numeric_column }},
            {{ date_column }}
        FROM
            `data-pipeline-project-445905`.`dbt_vkv`.`clean_sales`  -- Replace with your actual table reference or name
        WHERE
            {{ date_column }} BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    )
    
    SELECT
        {% if time_period == 'monthly' %}
            EXTRACT(MONTH FROM {{ date_column }}) AS period,
        {% elif time_period == 'quarterly' %}
            EXTRACT(QUARTER FROM {{ date_column }}) AS period,
        {% else %}
            EXTRACT(YEAR FROM {{ date_column }}) AS period,  -- Default to yearly aggregation
        {% endif %}
        
        SUM({{ numeric_column }}) AS total_value,
        AVG({{ numeric_column }}) AS average_value,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {{ numeric_column }}) AS median_value
    FROM
        data
    GROUP BY
        {% if time_period == 'monthly' %}
            EXTRACT(MONTH FROM {{ date_column }})
        {% elif time_period == 'quarterly' %}
            EXTRACT(QUARTER FROM {{ date_column }})
        {% else %}
            EXTRACT(YEAR FROM {{ date_column }})
        {% endif %}
    ORDER BY
        period;
{% endmacro %}
