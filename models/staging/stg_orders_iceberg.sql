{{ 
    config(
        materialized='table',
        catalog_name = 'stephen-r-iceberg'
    )
}}

select * from {{ref('stg_orders')}}