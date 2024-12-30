{{ 
  common_aggregations(
    table_name='`data-pipeline-project-445905.dbt_vkv.clean_sales`',
    date_column='sale_date',
    numeric_column='amount',
    period='quarter'
  ) 
}}
-- {{
--   common_aggregations(
--     table_name='`data-pipeline-project-445905.dbt_vkv.clean_products`',
--     date_column='order_date',
--     numeric_column='price',
--     period='quarter'
--   )
-- }}

-- UNION ALL

-- {{
--   common_aggregations(
--     table_name='`data-pipeline-project-445905.dbt_vkv.clean_sales`',
--     date_column='order_date',
--     numeric_column='amount',
--     period='year'
--   )
-- }}