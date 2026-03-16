with 

source as (

    select * from {{ source('raw_layer', 'employee_rw') }}
    {{ incremental_filter('record_date') }}
),

renamed as (

    select
       TO_DATE(record_date) AS record_date,

employee_id::VARCHAR AS employee_id,
first_name::VARCHAR AS first_name,
last_name::VARCHAR AS last_name,
email::VARCHAR AS email,
phone::VARCHAR AS phone,

date_of_birth::DATE AS date_of_birth,
hire_date::DATE AS hire_date,

employment_status::VARCHAR AS employment_status,
department::VARCHAR AS department,
role::VARCHAR AS role,

manager_id::VARCHAR AS manager_id,
work_location::VARCHAR AS work_location,

salary::VARCHAR AS salary,
current_sales::NUMBER AS current_sales,
sales_target::NUMBER AS sales_target,

performance_rating::VARCHAR AS performance_rating,

last_modified_date::DATE AS last_modified_date,

address::VARIANT AS address,
certifications::VARIANT AS certifications,
education::VARIANT AS education,

    from source

)

select * from renamed