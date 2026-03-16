{{ config(materialized='table') }}

SELECT

    MD5(CONCAT(employee_id)) AS employee_key,
    employee_id,
    Full_Name,
    Role,
    Work_location,
    Tenure_Years AS Tenure,
    Email,
    Phone,
    orders_processed,
    target_achievement_pct,
    total_sales_amount

FROM {{ ref('sl_employee') }}