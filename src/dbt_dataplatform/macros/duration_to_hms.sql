{% macro ms_to_hms(ms_column) %}
    CONCAT(
        CAST(FLOOR({{ ms_column }} / 3600000) AS STRING), 'h ',
        CAST(FLOOR(MOD({{ ms_column }}, 3600000) / 60000) AS STRING), 'm ',
        CAST(FLOOR(MOD({{ ms_column }}, 60000) / 1000) AS STRING), 's'
    )
{% endmacro %}

{% macro ms_to_hm(ms_column) %}
    CASE
        WHEN FLOOR({{ ms_column }} / 3600000) > 0 THEN
            CONCAT(
                CAST(FLOOR({{ ms_column }} / 3600000) AS STRING), 'h ',
                CAST(FLOOR(MOD({{ ms_column }}, 3600000) / 60000) AS STRING), 'm'
            )
        ELSE
            CONCAT(CAST(FLOOR({{ ms_column }} / 60000) AS STRING), 'm')
    END
{% endmacro %}