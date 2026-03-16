with source as (

    select *
    from {{ ref('store_rw') }} 

),

deduplicated as (

    select 
        TO_DATE(record_date) AS record_date,

store_id::VARCHAR AS store_id,
store_name::VARCHAR AS store_name,
store_type::VARCHAR AS store_type,
region::VARCHAR AS region,

manager_id::VARCHAR AS manager_id,

current_sales::NUMBER AS current_sales,
sales_target::NUMBER AS sales_target,
monthly_rent::VARCHAR AS monthly_rent,

employee_count::NUMBER AS employee_count,
size_sq_ft::NUMBER AS size_sq_ft,

opening_date::DATE AS opening_date,
last_modified_date::DATE AS last_modified_date,

is_active::BOOLEAN AS is_active,

email::VARCHAR AS email,
phone_number::VARCHAR AS phone_number,

address::VARIANT AS address,
operating_hours::VARIANT AS operating_hours,
services::VARIANT AS services,
        row_number() over (
            partition by store_id,record_date
            order by last_modified_date desc
        ) as rn

    from source

)

select *
from deduplicated
where rn = 1