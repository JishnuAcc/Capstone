{% macro validate_email(column) %}
CASE
    WHEN REGEXP_LIKE({{ column }}, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
    THEN LOWER({{ column }})
    ELSE NULL
END
{% endmacro %}