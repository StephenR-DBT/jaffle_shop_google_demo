select *
from {{ ref('stg_orders_iceberg') }}