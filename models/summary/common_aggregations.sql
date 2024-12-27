-- models/summary/common_aggregations.sql

{{ config(materialized='view') }}

-- Correctly call the macro without backticks around the table name
{{ common_aggregations('data-pipeline-project-445905.dbt_vkv.clean_sales', 'amount') }}

{{ common_aggregations('data-pipeline-project-445905.dbt_vkv.clean_products', 'price') }}