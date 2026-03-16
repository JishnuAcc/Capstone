with source as (

    select *
    from {{ ref('product_rw') }} 

),

deduplicated as (

    select 
        TO_DATE(record_date) AS record_date,

product_id::VARCHAR AS product_id,
name::VARCHAR AS name,
brand::VARCHAR AS brand,
category::VARCHAR AS category,
subcategory::VARCHAR AS subcategory,
product_line::VARCHAR AS product_line,

supplier_id::VARCHAR AS supplier_id,

color::VARCHAR AS color,
size::VARCHAR AS size,
weight::VARCHAR AS weight,

unit_price::VARCHAR AS unit_price,
cost_price::VARCHAR AS cost_price,

stock_quantity::NUMBER AS stock_quantity,
reorder_level::NUMBER AS reorder_level,

is_featured::BOOLEAN AS is_featured,

launch_date::DATE AS launch_date,
last_modified_date::DATE AS last_modified_date,

short_description::VARCHAR AS short_description,
warranty_period::VARCHAR AS warranty_period,

dimensions::VARIANT AS dimensions,
technical_specs::VARIANT AS technical_specs,
        row_number() over (
            partition by product_id,record_date
            order by last_modified_date desc
        ) as rn

    from source

)

select *
from deduplicated
where rn = 1