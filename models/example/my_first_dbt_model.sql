-- models/clean_sales.sql

WITH standardized_sales AS (
    SELECT
        -- Standardizing the sale_date format (e.g., YYYY-MM-DD)
        CAST(DATE_TRUNC(sale_date, DAY) AS DATE) AS sale_date,
        
        -- Handling null values in the amount column by replacing with 0 or a default value
        COALESCE(amount, 0) AS amount,
        
        -- Ensuring valid foreign key references (e.g., customer_id, product_id) using joins with reference tables
        customer_id,
        product_id,
        sale_id
        
    FROM {{ source('raw', 'raw_sales') }}
),

valid_foreign_keys AS (
    SELECT
        ss.sale_date,
        ss.amount,
        ss.customer_id,
        ss.product_id,
        ss.sale_id
        -- Ensuring valid foreign key by joining with customers and products tables
        CASE 
            WHEN c.customer_id IS NOT NULL THEN ss.customer_id
            ELSE NULL
        END AS valid_customer_id,
        
        CASE 
            WHEN p.product_id IS NOT NULL THEN ss.product_id
            ELSE NULL
        END AS valid_product_id

    FROM standardized_sales ss
    LEFT JOIN {{ source('raw', 'raw_customers') }} c
        ON ss.customer_id = c.customer_id
        
    LEFT JOIN {{ source('raw', 'raw_products') }} p
        ON ss.product_id = p.product_id
)

SELECT
    sale_date,
    amount,
    valid_customer_id AS customer_id,
    valid_product_id AS product_id
    sale_id
FROM valid_foreign_keys
WHERE valid_customer_id IS NOT NULL
  AND valid_product_id IS NOT NULL
