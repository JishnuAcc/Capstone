{{ config(materialized='table') }}

SELECT

    MD5(CONCAT(product_id)) AS product_key,

    product_id,
    name AS product_name,
    category,
    subcategory,
    brand,
    color,
    size,
    unit_price,
    cost_price,
    supplier_id AS supplier_information

FROM {{ ref('sl_product') }}