{% macro extract_weight(column) %}

TRY_TO_NUMBER(
    REGEXP_REPLACE({{ column }}, '[^0-9.]', '')
)

{% endmacro %}