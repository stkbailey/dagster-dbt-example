
-- Use the `ref` function to select from other models

select *
from {{ ref('pure_dbt_model') }}
where id = 1
