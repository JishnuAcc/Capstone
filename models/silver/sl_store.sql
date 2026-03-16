{{ config(materialized='incremental') }}

SELECT

    {{ standardize_date('record_date') }} AS record_date,

    store_id,

    INITCAP({{ clean_string('store_name') }}) AS store_name,
    {{ clean_string('store_type') }} AS store_type,
    {{ clean_string('region') }} AS region,

    manager_id,

    current_sales,
    sales_target,

    {{ extract_currency('monthly_rent') }} AS monthly_rent,

    employee_count,
    size_sq_ft,

    CASE
        WHEN size_sq_ft < 5000 THEN 'Small'
        WHEN size_sq_ft BETWEEN 5000 AND 10000 THEN 'Medium'
        WHEN size_sq_ft > 10000 THEN 'Large'
        ELSE 'Unknown'
    END AS store_size_category,

    {{ standardize_date('opening_date') }} AS opening_date,
    {{ standardize_date('last_modified_date') }} AS last_modified_date,

    DATEDIFF(year, {{ standardize_date('opening_date') }}, CURRENT_DATE()) AS store_age_years,

    is_active,

    {{ validate_email('email') }} AS email,
    {{ normalize_phone('phone_number') }} AS phone_number,

    OBJECT_CONSTRUCT(
        'street', address:street::varchar,
        'city', address:city::varchar,
        'state', address:state::varchar,
        'zip_code', REGEXP_REPLACE(address:zip_code::varchar,'[^0-9]',''),
        'country', address:country::varchar
    ) AS address,

    operating_hours,
    services,

    (current_sales / NULLIF(sales_target,0)) * 100 AS sales_target_achievement_percentage,

    (current_sales / NULLIF(size_sq_ft,0)) AS revenue_per_sq_ft,

    (current_sales / NULLIF(employee_count,0)) AS employee_efficiency,

    CASE
        WHEN (current_sales / NULLIF(sales_target,0)) * 100 < 90 THEN TRUE
        ELSE FALSE
    END AS performance_issue_flag

FROM {{ ref('br_store') }}

{% if is_incremental() %}
WHERE record_date > (SELECT MAX(record_date) FROM {{ this }})
{% endif %}