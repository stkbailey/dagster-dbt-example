select *
from {{ ref('seed_file_table') }}
where false