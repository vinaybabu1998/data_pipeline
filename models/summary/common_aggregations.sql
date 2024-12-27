{{ config(materialized='view') }}

-- Use the macro for 'clean_sales' table, 'amount' column, and 'MONTH' as time period (default)
{{ common_aggregations('clean_sales', 'amount') }}

-- -- Use the macro for 'clean_products' table, 'price' column, and 'QUARTER' as time period
-- {{ common_aggregations('clean_products', 'price', 'QUARTER') }}

-- -- Use the macro for 'clean_sales' table, 'amount' column, and 'YEAR' as time period
-- {{ common_aggregations('clean_sales', 'amount', 'YEAR') }}