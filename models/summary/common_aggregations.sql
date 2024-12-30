create or replace view `data-pipeline-project-445905`.`dbt_vkv`.`common_aggregations`
OPTIONS()
as 

{{ common_aggregations('`data-pipeline-project-445905.dbt_vkv.clean_sales`', 'amount', 'MONTH') }}

UNION ALL

{{ common_aggregations('`data-pipeline-project-445905.dbt_vkv.clean_products`', 'price', 'QUARTER') }}

UNION ALL

{{ common_aggregations('`data-pipeline-project-445905.dbt_vkv.clean_sales`', 'amount', 'YEAR') }}
;
