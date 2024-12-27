-- models/summary/common_aggregations_model.sql

{{ config(materialized='view') }}

-- Use the common_aggregations macro for the 'clean_sales' table and the 'amount' column
{{ common_aggregations('data-pipeline-project-445905.dbt_vkv.clean_sales', 'amount', 'month') }}

-- UNION ALL

-- -- Use the same macro for the 'clean_products' table and the 'price' column
-- SELECT * FROM {{ common_aggregations('data-pipeline-project-445905.dbt_vkv.clean_products', 'price', 'quarter') }}
