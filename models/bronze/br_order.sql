with source as (

    select *
    from {{ ref('order_rw') }} 

),

deduplicated as (

    select 
        TO_DATE(record_date) AS record_date,

order_id::VARCHAR AS order_id,
customer_id::VARCHAR AS customer_id,
campaign_id::VARCHAR AS campaign_id,
employee_id::VARCHAR AS employee_id,
store_id::VARCHAR AS store_id,

order_date::DATE AS order_date,
created_at::TIMESTAMP AS created_at,
delivery_date::DATE AS delivery_date,
estimated_delivery_date::DATE AS estimated_delivery_date,
shipping_date::DATE AS shipping_date,

order_status::VARCHAR AS order_status,
order_source::VARCHAR AS order_source,
payment_method::VARCHAR AS payment_method,
shipping_method::VARCHAR AS shipping_method,

shipping_cost::VARCHAR AS shipping_cost,
discount_amount::VARCHAR AS discount_amount,
tax_amount::VARCHAR AS tax_amount,
total_amount::VARCHAR AS total_amount,

billing_address::VARIANT AS billing_address,
shipping_address::VARIANT AS shipping_address,
order_items::VARIANT AS order_items,
        row_number() over (
            partition by order_id,record_date
            order by order_date desc
        ) as rn

    from source

)

select *
from deduplicated
where rn = 1