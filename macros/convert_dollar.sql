{% macro convert_dollar(column_name) %}
	{{column_name}}/100
{% endmacro %}