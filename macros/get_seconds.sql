{% macro get_seconds(column_name) %}
	cast(FLOOR( ( {{column_name}} - cast(FLOOR({{column_name}}) as INT64)) * 86400 ) as INT64)
{% endmacro %}