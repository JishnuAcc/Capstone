{{ config(materialized='table') }}

WITH orders AS (

    SELECT *
    FROM {{ ref('sl_order') }}

),

products AS (

    SELECT *
    FROM {{ ref('sl_product') }}

),

sales_agg AS (

    SELECT
        oi.value:product_id::VARCHAR AS product_id,
        o.store_id,
        DATE(o.order_date) AS order_date,
        SUM(oi.value:quantity::NUMBER) AS sold_quantity
    FROM orders o,
         LATERAL FLATTEN(input => o.order_items) oi
    GROUP BY
        oi.value:product_id::VARCHAR,
        o.store_id,
        DATE(o.order_date)

),

joined AS (

    SELECT
        p.product_id,
        p.supplier_id,
        sa.store_id,
        sa.order_date,
        sa.sold_quantity,
        p.stock_quantity,
        p.cost_price,
        p.reorder_level

    FROM sales_agg sa

    LEFT JOIN products p
        ON sa.product_id = p.product_id

)

SELECT

    ROW_NUMBER() OVER(ORDER BY dp.product_id) AS inventory_key,

    dp.product_key,
    dd.date_key,
    ds.store_key,
    sup.supplier_key,

    j.stock_quantity AS beginning_inventory,

    (j.stock_quantity - j.sold_quantity) AS ending_inventory,

    CASE
        WHEN (j.stock_quantity - j.sold_quantity) < j.reorder_level
        THEN j.reorder_level - (j.stock_quantity - j.sold_quantity)
        ELSE 0
    END AS purchased_quantity,

    j.sold_quantity,

    (j.stock_quantity - j.sold_quantity) * j.cost_price AS inventory_value,

    j.sold_quantity /
    NULLIF(
        (j.stock_quantity + (j.stock_quantity - j.sold_quantity)) / 2,
        0
    ) AS stock_turnover_ratio,

    100 *
    CASE
        WHEN (j.stock_quantity - j.sold_quantity) < j.reorder_level
        THEN j.reorder_level - (j.stock_quantity - j.sold_quantity)
        ELSE 0
    END
    /
    NULLIF(
        SUM(
            CASE
                WHEN (j.stock_quantity - j.sold_quantity) < j.reorder_level
                THEN j.reorder_level - (j.stock_quantity - j.sold_quantity)
                ELSE 0
            END
        ) OVER (PARTITION BY sup.supplier_key),
        0
    ) AS supplier_contribution_percentage

FROM joined j

LEFT JOIN {{ ref('dim_product') }} dp
    ON j.product_id = dp.product_id

LEFT JOIN {{ ref('dim_store') }} ds
    ON j.store_id = ds.store_id

LEFT JOIN {{ ref('dim_supplier') }} sup
    ON j.supplier_id = sup.supplier_id

LEFT JOIN {{ ref('dim_date') }} dd
    ON j.order_date = dd.full_date