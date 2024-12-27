-- models/summary/common_aggregations.sql

{{ config(materialized='view') }}

-- Replace with your actual table name and column
{{ common_aggregations(`data-pipeline-project-445905`.`dbt_vkv`.`clean_sales`, 'amount') }}
