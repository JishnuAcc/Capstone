{% macro clean_string(column) %}

INITCAP(
    REGEXP_REPLACE(
        TRIM({{ column }}),
        '[^a-zA-Z0-9@._ -]',
        ''
    )
)

{% endmacro %}