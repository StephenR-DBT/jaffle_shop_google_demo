with 

source as (
    select * from {{ ref('regional_sales_targets') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['location_id', 'target_month']) }} as target_id,
        
        --------- ids
        cast(location_id as string) as location_id,

        --------- dates
        cast(target_month as date) as target_month,

        --------- numerics
        cast(monthly_revenue_target as float64) as monthly_revenue_target
    from source
)

select * from renamed
