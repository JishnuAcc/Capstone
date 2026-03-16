{{ config(materialized='table') }}

SELECT

    MD5(CONCAT(customer_id,'-',dbt_valid_from)) AS customer_key,
    customer_id,
    full_name,
    email,
    phone,
    address,
    customer_segment AS segment,
    registration_date,
    dbt_valid_from AS effective_from,
    dbt_valid_to AS effective_to
FROM {{ ref('customer_snapshot') }}