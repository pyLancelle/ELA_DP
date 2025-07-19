{% macro get_schema(layer) %}
  {% if layer == 'lake' %}
    {{ return('lake_' ~ target.name) }}
  {% elif layer == 'hub' %}
    {{ return('hub_' ~ target.name) }}
  {% elif layer == 'product' %}
    {{ return('product_' ~ target.name) }}
  {% else %}
    {{ exceptions.raise_compiler_error("Invalid layer: " ~ layer) }}
  {% endif %}
{% endmacro %}
