with 

source as (

    select * from {{ source('raw_layer', 'product_rw') }}
    {{ incremental_filter('record_date') }}
),

renamed as (

    select
        record_date,
        product_id,
        name,
        brand,
        category,
        subcategory,
        product_line,
        supplier_id,
        color,
        size,
        weight,
        unit_price,
        cost_price,
        stock_quantity,
        reorder_level,
        is_featured,
        launch_date,
        last_modified_date,
        short_description,
        warranty_period,
        dimensions,
        technical_specs

    from source

)

select * from renamed