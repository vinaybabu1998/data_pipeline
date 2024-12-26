WITH standardized_customers AS (
    SELECT
        -- Standardizing the signup_date format (e.g., YYYY-MM-DD)
        CAST(DATE_TRUNC(signup_date, DAY) AS DATE) AS signup_date,
        
        -- Handling null values in the name column by replacing with an empty string
        COALESCE(name, "") AS name,
        
        -- Ensuring valid foreign key references (e.g., customer_id) using joins with reference tables
        customer_id,
        region
        
    FROM `data-pipeline-project-445905`.`big_query_id`.`raw_customers`
),

valid_foreign_keys AS (
    SELECT
        cs.signup_date,
        cs.name,
        cs.customer_id,
        -- Removed sale_id as it is not part of standardized_customers
        CASE 
            WHEN s.customer_id IS NOT NULL THEN cs.customer_id
            ELSE NULL
        END AS valid_customer_id

    FROM standardized_customers cs
    LEFT JOIN `data-pipeline-project-445905`.`big_query_id`.`raw_sales` s
        ON cs.customer_id = s.customer_id
)

SELECT
    customer_id,
    signup_date,
    name,
    valid_customer_id AS customer_id
FROM valid_foreign_keys
WHERE valid_customer_id IS NOT NULL;
