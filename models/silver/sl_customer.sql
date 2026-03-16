{{ config(materialized='incremental') }}

SELECT

    {{ standardize_date('record_date') }} AS record_date,

    customer_id,

    {{ clean_string('first_name') }} AS first_name,
    {{ clean_string('last_name') }} AS last_name,

    CONCAT(
        {{ clean_string('first_name') }},
        ' ',
        {{ clean_string('last_name') }}
    ) AS full_name,

    {{ validate_email('email') }} AS email,
    {{ normalize_phone('phone') }} AS phone,

    {{ standardize_date('birth_date') }} AS birth_date,

    /* Customer age */
    DATEDIFF(
        year,
        {{ standardize_date('birth_date') }},
        CURRENT_DATE()
    ) AS customer_age,

    /* Customer segment */
    CASE
        WHEN DATEDIFF(year, {{ standardize_date('birth_date') }}, CURRENT_DATE()) BETWEEN 18 AND 35 THEN 'Young'
        WHEN DATEDIFF(year, {{ standardize_date('birth_date') }}, CURRENT_DATE()) BETWEEN 36 AND 55 THEN 'Middle-aged'
        WHEN DATEDIFF(year, {{ standardize_date('birth_date') }}, CURRENT_DATE()) > 55 THEN 'Senior'
        ELSE 'Unknown'
    END AS customer_segment,

    {{ clean_string('occupation') }} AS occupation,
    {{ clean_string('income_bracket') }} AS income_bracket,
    {{ clean_string('loyalty_tier') }} AS loyalty_tier,

    marketing_opt_in,

    {{ clean_string('preferred_communication') }} AS preferred_communication,
    {{ clean_string('preferred_payment_method') }} AS preferred_payment_method,

    {{ standardize_date('registration_date') }} AS registration_date,
    {{ standardize_date('last_purchase_date') }} AS last_purchase_date,
    {{ standardize_date('last_modified_date') }} AS last_modified_date,

    total_purchases,
    {{ extract_currency('total_spend') }} AS total_spend,

    OBJECT_CONSTRUCT(
        'street', address:street::varchar,
        'city', address:city::varchar,
        'state', address:state::varchar,
        'zip_code', address:zip_code::varchar,
        'country', address:country::varchar
    ) AS address

FROM {{ ref('br_customer') }}

{% if is_incremental() %}
WHERE record_date > (SELECT MAX(record_date) FROM {{ this }})
{% endif %}