-- macros/seconds_to_hms.sql
{% macro seconds_to_hms(column_name) %}
    format('%02d:%02d:%02d',
        div(cast({{ column_name }} as int64), 3600),
        div(mod(cast({{ column_name }} as int64), 3600), 60),
        mod(cast({{ column_name }} as int64), 60)
    )
{% endmacro %}