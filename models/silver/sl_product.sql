{{ config(materialized='incremental') }}

SELECT

    {{ standardize_date('record_date') }} AS record_date,

    product_id,

    INITCAP({{ clean_string('name') }}) AS name,
    INITCAP({{ clean_string('brand') }}) AS brand,
    INITCAP({{ clean_string('category') }}) AS category,
    INITCAP({{ clean_string('subcategory') }}) AS subcategory,
    INITCAP({{ clean_string('product_line') }}) AS product_line,

    supplier_id,

    INITCAP({{ clean_string('color') }}) AS color,
    INITCAP({{ clean_string('size') }}) AS size,

    {{ extract_weight('weight') }} AS weight,

    {{ extract_currency('unit_price') }} AS unit_price,
    {{ extract_currency('cost_price') }} AS cost_price,

    stock_quantity,
    reorder_level,

    is_featured,

    {{ standardize_date('launch_date') }} AS launch_date,
    {{ standardize_date('last_modified_date') }} AS last_modified_date,

    {{ clean_string('short_description') }} AS short_description,
    {{ clean_string('warranty_period') }} AS warranty_period,

    dimensions,
    technical_specs,

    /* Full description */
    CONCAT(
        INITCAP({{ clean_string('name') }}),
        ' - ',
        {{ clean_string('short_description') }},
        ' - ',
        TO_VARCHAR(technical_specs)
    ) AS full_description,

    /* Hierarchical category */
    CONCAT(
        INITCAP({{ clean_string('category') }}),
        ' > ',
        INITCAP({{ clean_string('subcategory') }}),
        ' > ',
        INITCAP({{ clean_string('product_line') }})
    ) AS category_hierarchy,

    /* Profit margin */
    (
        ({{ extract_currency('unit_price') }} - {{ extract_currency('cost_price') }})
        / NULLIF({{ extract_currency('unit_price') }},0)
    ) * 100 AS profit_margin,

    /* Low stock flag */
    CASE
        WHEN stock_quantity < reorder_level THEN TRUE
        ELSE FALSE
    END AS low_stock_flag

FROM {{ ref('br_product') }}

{% if is_incremental() %}
WHERE record_date > (SELECT MAX(record_date) FROM {{ this }})
{% endif %}
ORDER BY category_hierarchy