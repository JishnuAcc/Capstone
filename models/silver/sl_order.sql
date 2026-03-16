{{ config(materialized='incremental') }}

WITH base AS (
SELECT
    {{ standardize_date('record_date') }} AS record_date,
    order_id,
    customer_id,
    campaign_id,
    employee_id,
    store_id,
    {{ standardize_date('order_date') }} AS order_date,
    created_at,
    {{ standardize_date('delivery_date') }} AS delivery_date,
    {{ standardize_date('estimated_delivery_date') }} AS estimated_delivery_date,
    {{ standardize_date('shipping_date') }} AS shipping_date,
    {{ clean_string('order_status') }} AS order_status,
    {{ clean_string('order_source') }} AS order_source,
    {{ clean_string('payment_method') }} AS payment_method,
    {{ clean_string('shipping_method') }} AS shipping_method,
    {{ extract_currency('shipping_cost') }} AS shipping_cost,
    {{ extract_currency('discount_amount') }} AS discount_amount,
    {{ extract_currency('tax_amount') }} AS tax_amount,
    {{ extract_currency('total_amount') }} AS total_amount,
    billing_address,
    shipping_address,
    order_items
FROM {{ ref('br_order') }}
),
profit_calc AS (
SELECT
    b.*,
    (
        total_amount
        - COALESCE(discount_amount,0)
        - COALESCE(shipping_cost,0)
        - COALESCE(tax_amount,0)
    ) AS profit_amount
FROM base b
)

SELECT
    *,
    profit_amount / NULLIF(total_amount,0) * 100 AS profit_margin_percentage,
    CASE
        WHEN DATE_PART(hour, created_at) BETWEEN 5 AND 11 THEN 'Morning'
        WHEN DATE_PART(hour, created_at) BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN DATE_PART(hour, created_at) BETWEEN 17 AND 21 THEN 'Evening'
        ELSE 'Night'
    END AS order_time_of_day,
    DATE_TRUNC('week', order_date) AS order_week,
    TO_CHAR(order_date,'MONTH') AS order_month,
    ROUND(MONTH(order_date)/3)+1 AS order_quarter,
    YEAR(order_date) AS order_year,
    DATEDIFF(day, order_date, shipping_date) AS processing_days,
    DATEDIFF(day, shipping_date, delivery_date) AS shipping_days,

    CASE
        WHEN delivery_date IS NOT NULL
             AND delivery_date <= estimated_delivery_date
        THEN 'On Time'

        WHEN delivery_date IS NOT NULL
             AND delivery_date > estimated_delivery_date
        THEN 'Delayed'

        WHEN delivery_date IS NULL
             AND CURRENT_DATE() > estimated_delivery_date
        THEN 'Potentially Delayed'

        ELSE 'In Transit'
    END AS delivery_status

FROM profit_calc

{% if is_incremental() %}
WHERE record_date > (SELECT MAX(record_date) FROM {{ this }})
{% endif %}