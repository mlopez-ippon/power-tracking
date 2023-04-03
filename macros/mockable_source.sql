{% macro mockable_source(env, test_input, real_input) %}
    {% if env == 'ci' %}
        select * from {{ ref(test_input) }}
    {% else %}
        select * from {{ ref(real_input) }}
    {% endif %}
{% endmacro %}