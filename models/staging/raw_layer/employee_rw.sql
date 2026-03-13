with 

source as (

    select * from {{ source('raw_layer', 'employee_rw') }}
    {{ incremental_filter('record_date') }}
),

renamed as (

    select
        record_date,
        employee_id,
        first_name,
        last_name,
        email,
        phone,
        date_of_birth,
        hire_date,
        employment_status,
        department,
        role,
        manager_id,
        work_location,
        salary,
        current_sales,
        sales_target,
        performance_rating,
        last_modified_date,
        address,
        certifications,
        education

    from source

)

select * from renamed