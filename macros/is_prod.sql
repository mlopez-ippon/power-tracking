{% macro is_prod() %}

    {% if target.name.lower() in ["dev", "ci"] %}
        {{ return(false) }}
    {% else %}
        {{ return(true) }}
    {% endif %}

{% endmacro %}