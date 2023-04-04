{% macro mockable_source(test_input, real_schema, real_table) %}
    {% if target.name.lower() == 'ci' %}
        select * from {{ ref(test_input) }}
    {% else %}
        select * from {{ source(real_schema,real_table) }}
    {% endif %}
{% endmacro %}