{% macro calculate_average(column_name) %}
    ROUND(AVG({{ column_name }}),2)
{% endmacro %}