{% macro handle_null(column, default_value) %}
COALESCE({{ column }}, {{ default_value }})
{% endmacro %}