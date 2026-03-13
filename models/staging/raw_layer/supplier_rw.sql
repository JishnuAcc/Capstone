with 
source as (
    select * from {{ source('raw_layer', 'supplier_rw') }}
    {{ incremental_filter('record_date') }}
),
renamed as (
    select
        record_date,
        supplier_id,
        supplier_name,
        supplier_type,
        tax_id,
        website,
        year_established,
        credit_rating,
        is_active,
        lead_time_days,
        minimum_order_quantity,
        payment_terms,
        preferred_carrier,
        last_order_date,
        last_modified_date,
        categories_supplied,
        contact_information,
        contract_details,
        performance_metrics
    from source

)
select * from renamed