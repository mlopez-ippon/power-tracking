{% macro is_prod() %}

    {% if target.name in ["dev", "ci", "CI"] %}
        {{ return(false) }}
    {% else %}
        {{ return(true) }}
    {% endif %}

{% endmacro %}