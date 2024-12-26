{% macro sale_date_format(date_column) %}
  format_date('%Y-%m-%d', {{ date_column }})
{% endmacro %}
