{% macro convert_local(column_name) %}
	DATETIME({{column_name}},'America/Denver')
{% endmacro %}