{{
  config(
    labels = {'contains_pii': 'yes'}
  )
}}

with

customers as (

    select * from {{ ref('stg_customers') }}

),

customer_info as (

    select * from {{ ref('stg_customer_info') }}

),

orders as (

    select * from {{ ref('orders') }}

),

customer_orders_summary as (

    select
        customer_id,

        count(distinct order_id)                as count_lifetime_orders,
        count(distinct order_id) > 1            as is_repeat_buyer,
        min(order_date)                         as first_order_date,
        max(order_date)                         as last_order_date,
        sum(subtotal)                           as lifetime_spend_pretax,
        sum(tax_paid)                           as lifetime_tax_paid,
        sum(order_total)                        as lifetime_spend

    from orders
    group by 1

),

joined as (

    select
        ---------- identity
        customers.customer_id,
        customers.customer_name,

        ---------- contact & address (PII)
        customer_info.email,
        customer_info.phone,
        customer_info.street_address,
        customer_info.city,
        customer_info.state,
        customer_info.zip_code,
        customer_info.country,

        ---------- payment (PII)
        customer_info.credit_card_number,
        customer_info.credit_card_expiry,
        customer_info.credit_card_type,

        ---------- demographics (PII)
        customer_info.date_of_birth,

        ---------- order history
        coalesce(customer_orders_summary.count_lifetime_orders, 0)  as count_lifetime_orders,
        customer_orders_summary.first_order_date,
        customer_orders_summary.last_order_date,
        coalesce(customer_orders_summary.lifetime_spend_pretax, 0)  as lifetime_spend_pretax,
        coalesce(customer_orders_summary.lifetime_tax_paid, 0)      as lifetime_tax_paid,
        coalesce(customer_orders_summary.lifetime_spend, 0)         as lifetime_spend,

        ---------- derived
        case
            when customer_orders_summary.is_repeat_buyer then 'returning'
            else 'new'
        end as customer_type

    from customers
    left join customer_info
        on customers.customer_id = customer_info.customer_id
    left join customer_orders_summary
        on customers.customer_id = customer_orders_summary.customer_id

)

select * from joined
