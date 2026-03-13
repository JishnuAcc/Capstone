with 

source as (

    select * from {{ source('raw_layer', 'store_rw') }}
    {{ incremental_filter('record_date') }}
),

renamed as (

    select
        record_date,
        store_id,
        store_name,
        store_type,
        region,
        manager_id,
        current_sales,
        sales_target,
        monthly_rent,
        employee_count,
        size_sq_ft,
        opening_date,
        last_modified_date,
        is_active,
        email,
        phone_number,
        address,
        operating_hours,
        services

    from source

)

select * from renamed