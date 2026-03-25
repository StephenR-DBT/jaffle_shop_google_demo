{{ config(
    materialized='table'
) }}

with

orders as (
    select * from {{ ref('orders') }}
),

targets as (
    select * from {{ ref('stg_regional_targets') }}
),

order_revenue_by_month as (
    select
        location_id,
        date_trunc(cast(order_date as date), month) as target_month,
        sum(order_total) as actual_revenue
    from orders
    group by 1, 2
),

final as (
    select
        targets.target_id,
        targets.location_id,
        targets.target_month,
        coalesce(order_revenue_by_month.actual_revenue, 0) as actual_revenue,
        targets.monthly_revenue_target,
        
        case 
            when targets.monthly_revenue_target > 0 
                then (coalesce(order_revenue_by_month.actual_revenue, 0) / targets.monthly_revenue_target) * 100
            else null 
        end as revenue_attainment_pct,
        
        coalesce(order_revenue_by_month.actual_revenue, 0) - targets.monthly_revenue_target as revenue_vs_target_delta
        
    from targets
    left join order_revenue_by_month
        on targets.location_id = order_revenue_by_month.location_id
        and targets.target_month = order_revenue_by_month.target_month
)

select * from final
