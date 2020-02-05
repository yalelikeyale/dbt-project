{% macro get_date(column_name) %}
	DATE_ADD(DATE '1899-12-30', INTERVAL cast(FLOOR({{column_name}}) as INT64) DAY )
{% endmacro %}