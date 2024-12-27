-- models/clean_sales.sql

WITH standardized_products AS (
    SELECT
        product_name,

        -- Handling null values in the amount column by replacing with 0 or a default value
        COALESCE(price, 0) AS price,
        
        -- Ensuring valid foreign key references (e.g., customer_id) using joins with reference tables
        product_id,
        category
        
    FROM {{ source('raw', 'raw_products') }}
),

valid_foreign_keys AS (
    SELECT
        ps.product_name,
        ps.category,
        ps.product_id,
        ps.price
        -- Ensuring valid foreign key by joining with sales table
        CASE 
            WHEN ps.product_id IS NOT NULL THEN s.product_id
            ELSE NULL
        END AS valid_product_id

    FROM standardized_products ps
    LEFT JOIN {{ source('raw', 'raw_sales') }} s
        ON ps.product_id = s.product_id

)

SELECT
    product_id,
    product_name,
    category,
    price
FROM valid_foreign_keys
WHERE valid_product_id IS NOT NULL
