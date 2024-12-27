-- models/customer_lifetime_value.sql

{{ config(
    materialized='view'
) }}

WITH customer_spending AS (
    SELECT
        sales.customer_id,
        SUM(products.price) AS total_spending
    FROM `data-pipeline-project-445905`.`dbt_vkv`.`clean_sales` AS sales
    JOIN `data-pipeline-project-445905`.`dbt_vkv`.`clean_products` AS products
        ON sales.product_id = products.product_id
    GROUP BY
        sales.customer_id
)

SELECT
    customers.customer_id,
    customers.name,
    spending.total_spending
FROM customer_spending AS spending
JOIN `data-pipeline-project-445905`.`dbt_vkv`.`clean_customers` AS customers
    ON spending.customer_id = customers.customer_id
ORDER BY
    spending.total_spending DESC
