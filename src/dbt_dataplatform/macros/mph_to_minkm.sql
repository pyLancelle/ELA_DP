-- macros/ms_to_min_per_km.sql
{% macro ms_to_min_per_km(column_name) %}
    case 
        when {{ column_name }} is null or {{ column_name }} <= 0 
        then null
        else concat(
            cast(floor(1000 / ({{ column_name }} * 60)) as int64),
            ':',
            format('%02d', cast(mod(cast(round(1000 / {{ column_name }}) as int64), 60) as int64)),
            '/km'
        )
    end
{% endmacro %}