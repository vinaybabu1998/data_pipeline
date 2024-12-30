WITH aggregated_data AS (
    {{ calculate_aggregations('price', 'sale_date', '2024-01-01', '2024-12-31', 'monthly') }}
)

SELECT * FROM aggregated_data;
