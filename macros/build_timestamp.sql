{% macro build_timestamp(column_name) %}
    TIMESTAMP(
        DATETIME(
          EXTRACT(YEAR FROM {{column_name}})
        , EXTRACT(MONTH FROM {{column_name}})
        , EXTRACT(DAY FROM {{column_name}})
        , 0
        , 0
        , 0
      ),
      'America/Denver'
    )
{% endmacro %}