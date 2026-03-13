{% macro incremental_filter(date_column='record_date') %}

{% if is_incremental() %}

WHERE {{ date_column }} >
(
    SELECT MAX({{ date_column }})
    FROM {{ this }}
)

{% endif %}

{% endmacro %}