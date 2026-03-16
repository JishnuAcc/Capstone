{{ config(materialized='incremental') }}

SELECT

    {{ standardize_date('record_date') }} AS record_date,

    supplier_id,

    {{ clean_string('supplier_name') }} AS supplier_name,
    {{ clean_string('supplier_type') }} AS supplier_type,

    tax_id,
    website,

    year_established,

    credit_rating,
    is_active,

    lead_time_days,
    minimum_order_quantity,

    {{ clean_string('payment_terms') }} AS payment_terms,
    {{ clean_string('preferred_carrier') }} AS preferred_carrier,

    {{ standardize_date('last_order_date') }} AS last_order_date,
    {{ standardize_date('last_modified_date') }} AS last_modified_date,

    categories_supplied,
    contact_information,
    contract_details,
    performance_metrics

FROM {{ ref('br_supplier') }}

{% if is_incremental() %}
WHERE record_date > (SELECT MAX(record_date) FROM {{ this }})
{% endif %}