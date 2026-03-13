with 

source as (

    select * from {{ source('raw_layer', 'customer_rw') }}
    {{ incremental_filter('record_date') }}
),

renamed as (

    select
        record_date,
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        birth_date,
        occupation,
        income_bracket,
        loyalty_tier,
        marketing_opt_in,
        preferred_communication,
        preferred_payment_method,
        registration_date,
        last_purchase_date,
        last_modified_date,
        total_purchases,
        total_spend,
        address

    from source

)

select * from renamed