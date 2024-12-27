-- models/sales_summary.sql

WITH monthly_sales AS (
    SELECT
        -- Extracting the year and month from the sale_date
        FORMAT_TIMESTAMP('%Y-%m', sale_date) AS sale_month,
        products.category,
        COUNT(sales.sale_id) AS total_sales, -- Using the count of sales IDs as total sales
        SUM(products.price) AS total_revenue -- Summing up prices for revenue calculation
    FROM `data-pipeline-project-445905`.`big_query_id`.`raw_sales` AS sales
    JOIN `data-pipeline-project-445905`.`dbt_vkv`.`clean_products` AS products
        ON sales.product_id = products.product_id
    GROUP BY
        sale_month, products.category
)

SELECT
    sale_month,
    category,
    total_sales,
    total_revenue
FROM monthly_sales
ORDER BY
    sale_month, category
