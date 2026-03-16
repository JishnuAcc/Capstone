{% macro standardize_date(column) %}

TRY_TO_DATE({{ column }})

{% endmacro %}