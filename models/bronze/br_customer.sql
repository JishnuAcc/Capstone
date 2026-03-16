with source as (

    select *
    from {{ ref('customer_rw') }} 

),

deduplicated as (

    select 
        TO_DATE(record_date) AS record_date,
        customer_id::VARCHAR AS customer_id,
        first_name::VARCHAR AS first_name,
        last_name::VARCHAR AS last_name,
        email::VARCHAR AS email,
        phone::VARCHAR AS phone,

        birth_date::VARCHAR AS birth_date,

        occupation::VARCHAR AS occupation,
        income_bracket::VARCHAR AS income_bracket,
        loyalty_tier::VARCHAR AS loyalty_tier,

        marketing_opt_in::BOOLEAN AS marketing_opt_in,

        preferred_communication::VARCHAR AS preferred_communication,
        preferred_payment_method::VARCHAR AS preferred_payment_method,

        registration_date::DATE AS registration_date,
        last_purchase_date::DATE AS last_purchase_date,
        last_modified_date::DATE AS last_modified_date,

        total_purchases::NUMBER AS total_purchases,
        total_spend::VARCHAR AS total_spend,

        address::VARIANT AS address,
        row_number() over (
            partition by customer_id,record_date
            order by last_modified_date desc
        ) as rn

    from source

)

select *
from deduplicated
where rn = 1