with

source as (

    -- References the customer_info seed which holds PII enrichment data
    select * from {{ ref('customer_info') }}

),

renamed as (

    select

        ---------- ids
        customer_id,

        ---------- contact
        email,
        phone,

        ---------- address
        street_address,
        city,
        state,
        zip_code,
        country,

        ---------- payment (PII)
        credit_card_number,
        credit_card_expiry,
        credit_card_type,

        ---------- demographics (PII)
        cast(date_of_birth as date) as date_of_birth

    from source

)

select * from renamed
