with source as (

    select *
    from {{ ref('supplier_rw') }} 

),

deduplicated as (

    select 
        TO_DATE(record_date) AS record_date,

supplier_id::VARCHAR AS supplier_id,
supplier_name::VARCHAR AS supplier_name,
supplier_type::VARCHAR AS supplier_type,

tax_id::VARCHAR AS tax_id,
website::VARCHAR AS website,

year_established::NUMBER AS year_established,

credit_rating::VARCHAR AS credit_rating,
is_active::BOOLEAN AS is_active,

lead_time_days::NUMBER AS lead_time_days,
minimum_order_quantity::NUMBER AS minimum_order_quantity,

payment_terms::VARCHAR AS payment_terms,
preferred_carrier::VARCHAR AS preferred_carrier,

last_order_date::DATE AS last_order_date,
last_modified_date::DATE AS last_modified_date,

categories_supplied::VARIANT AS categories_supplied,
contact_information::VARIANT AS contact_information,
contract_details::VARIANT AS contract_details,
performance_metrics::VARIANT AS performance_metrics,
        row_number() over (
            partition by supplier_id,record_date
            order by last_modified_date desc
        ) as rn

    from source

)

select *
from deduplicated
where rn = 1