{% macro normalize_phone(column) %}
REGEXP_REPLACE({{ column }}, '[^0-9]', '')
{% endmacro %}