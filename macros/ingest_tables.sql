
{% macro ingest_all_tables() %}
{% for t in var('ingest_tables')%}
{{ load_json_folder(t)}}
{% endfor %}
{%- endmacro %}