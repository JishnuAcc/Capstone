{{ config(materialized='incremental') }}

WITH employee_base AS (

SELECT

    {{ standardize_date('record_date') }} AS record_date,

    employee_id,

    {{ clean_string('first_name') }} AS first_name,
    {{ clean_string('last_name') }} AS last_name,

    CONCAT(
        {{ clean_string('first_name') }},
        ' ',
        {{ clean_string('last_name') }}
    ) AS full_name,

    {{ validate_email('email') }} AS email,
    {{ normalize_phone('phone') }} AS phone,

    {{ standardize_date('date_of_birth') }} AS date_of_birth,
    {{ standardize_date('hire_date') }} AS hire_date,

    /* Tenure in years */
    DATEDIFF(year, {{ standardize_date('hire_date') }}, CURRENT_DATE()) AS tenure_years,

    {{ clean_string('employment_status') }} AS employment_status,
    {{ clean_string('department') }} AS department,

    /* Standardized role */
    CASE
        WHEN LOWER(role) LIKE '%sales associate%' THEN 'Associate'
        WHEN LOWER(role) LIKE '%senior manager%' THEN 'Senior Manager'
        WHEN LOWER(role) LIKE '%store manager%' THEN 'Manager'
        ELSE {{ clean_string('role') }}
    END AS role,

    manager_id,

    {{ clean_string('work_location') }} AS work_location,

    {{ extract_currency('salary') }} AS salary,
    current_sales,
    sales_target,

    /* Target achievement percentage */
    (current_sales / NULLIF(sales_target,0)) * 100 AS target_achievement_pct,

    {{ clean_string('performance_rating') }} AS performance_rating,

    {{ standardize_date('last_modified_date') }} AS last_modified_date,

    address,
    certifications,
    education

FROM {{ ref('br_employee') }}

),

order_metrics AS (

SELECT
    employee_id,
    COUNT(order_id) AS orders_processed,
    SUM({{ extract_currency('total_amount') }}) AS total_sales_amount
FROM {{ ref('sl_order') }}
GROUP BY employee_id

)

SELECT

    e.*,

    COALESCE(o.orders_processed,0) AS orders_processed,
    COALESCE(o.total_sales_amount,0) AS total_sales_amount

FROM employee_base e
LEFT JOIN order_metrics o
ON e.employee_id = o.employee_id


{% if is_incremental() %}
WHERE record_date > (SELECT MAX(record_date) FROM {{ this }})
{% endif %}