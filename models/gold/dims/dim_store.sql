{{ config(materialized='table') }}

SELECT

    MD5(CONCAT(store_id)) AS store_key,

    store_id,
    store_name,
    address,
    region,
    store_type,
    opening_date,
    store_size_category AS size_category

FROM {{ ref('sl_store') }}