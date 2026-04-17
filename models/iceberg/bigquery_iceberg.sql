{{
  config(
    labels = {'iceberg': 'yes'}
  )
}}

select *
from {{ ref('stg_orders_iceberg') }}