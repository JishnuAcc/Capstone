{{ config(materialized='table') }}

SELECT

    MD5(CONCAT(o.order_id, oi.value:product_id::VARCHAR)) AS sales_key,

    o.order_id,

    dc.customer_key,
    dp.product_key,
    ds.store_key,
    dd.date_key,
    de.employee_key,
    dmc.campaign_key,

    oi.value:quantity::NUMBER AS quantity_sold,

    oi.value:unit_price::NUMBER AS unit_price,

    oi.value:quantity::NUMBER * oi.value:unit_price::NUMBER AS total_sales_amount,

    oi.value:quantity::NUMBER * dp.cost_price AS cost_amount,

    (
        (oi.value:quantity::NUMBER * oi.value:unit_price::NUMBER)
        - (oi.value:quantity::NUMBER * dp.cost_price)
        - o.discount_amount
        - o.shipping_cost
    ) AS profit_amount,

    o.discount_amount,
    o.shipping_cost,

    ds.region,

    CASE
        WHEN LOWER(o.order_source) LIKE '%online%' THEN 'Online'
        ELSE 'In-Store'
    END AS sales_channel,

    dc.segment AS customer_segment_impact

FROM {{ ref('sl_order') }} o

LEFT JOIN LATERAL FLATTEN(input => o.order_items) oi

LEFT JOIN {{ ref('dim_customer') }} dc
    ON o.customer_id = dc.customer_id
    AND dc.effective_to IS NULL

LEFT JOIN {{ ref('dim_product') }} dp
    ON oi.value:product_id::VARCHAR = dp.product_id

LEFT JOIN {{ ref('dim_store') }} ds
    ON o.store_id = ds.store_id

LEFT JOIN {{ ref('dim_date') }} dd
    ON o.order_date = dd.full_date

LEFT JOIN {{ ref('dim_employee') }} de
    ON o.employee_id = de.employee_id

LEFT JOIN {{ ref('dim_marketingCampaign') }} dmc
    ON o.campaign_id = dmc.campaign_id