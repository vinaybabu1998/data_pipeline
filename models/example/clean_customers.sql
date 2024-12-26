-- models/clean_sales.sql

WITH standardized_customers AS (
    SELECT
        -- Standardizing the sale_date format (e.g., YYYY-MM-DD)
        CAST(DATE_TRUNC(signup_date, DAY) AS DATE) AS signup_date,
        
        -- Handling null values in the amount column by replacing with 0 or a default value
        COALESCE(name, "") AS name,
        
        -- Ensuring valid foreign key references (e.g., customer_id) using joins with reference tables
        customer_id,
        region
        
    FROM {{ source('raw', 'raw_customers') }}
),

valid_foreign_keys AS (
    SELECT
        cs.signup_date,
        cs.name,
        cs.region,
        -- Ensuring valid foreign key by joining with customers table
        CASE 
            WHEN cs.customer_id IS NOT NULL THEN s.customer_id
            ELSE NULL
        END AS valid_customer_id

    FROM standardized_customers cs
    LEFT JOIN {{ source('raw', 'raw_sales') }} s
        ON cs.customer_id = s.customer_id

)

SELECT
    valid_customer_id AS customer_id,
    signup_date,
    name,
    region
FROM valid_foreign_keys
WHERE valid_customer_id IS NOT NULL
