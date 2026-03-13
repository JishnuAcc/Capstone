
{% macro load_json_folder(table_name) %}
{% set sql %}
USE SCHEMA CP_BRONZE;
COPY INTO RAW_JSON
FROM (
    SELECT
        '{{ table_name }}' AS table_name,
        METADATA$FILENAME,
        CURRENT_TIMESTAMP(),
        $1
    FROM @CP_BRONZE_RW/{{table_name}}
)
FILE_FORMAT = (TYPE = JSON)
{% endset %}
{% do run_query(sql)%}
{%- endmacro %}