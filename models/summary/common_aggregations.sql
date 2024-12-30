{{ 
  common_aggregations(
    table_name='`data-pipeline-project-445905.dbt_vkv.clean_sales`',
    date_column='sale_date',
    numeric_column='amount',
    period='quarter'
  ) 
}}
