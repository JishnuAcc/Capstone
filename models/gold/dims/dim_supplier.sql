{{ config(materialized='table') }}

SELECT

    MD5(CONCAT(supplier_id)) AS supplier_key,

    supplier_id,
    supplier_name,
    Contact_Information,
    Payment_Terms,
    Supplier_Type

FROM {{ ref('sl_supplier') }}