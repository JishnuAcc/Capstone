with 

source as (

    select * from {{ source('raw_layer', 'order_rw') }}
    {{ incremental_filter('record_date') }}
),

renamed as (

    select
        record_date,
        order_id,
        customer_id,
        campaign_id,
        employee_id,
        store_id,
        order_date,
        created_at,
        delivery_date,
        estimated_delivery_date,
        shipping_date,
        order_status,
        order_source,
        payment_method,
        shipping_method,
        shipping_cost,
        discount_amount,
        tax_amount,
        total_amount,
        billing_address,
        shipping_address,
        order_items

    from source

)

select * from renamed