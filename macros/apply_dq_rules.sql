{% macro apply_dq_rules(target_table_name) %}

    {# 1. Формируем запрос для получения правил конкретной таблицы #}
    {% set get_rules_query %}
        SELECT column_name, rule_type, rule_value, error_message
        FROM {{ ref('dq_rules') }}
        WHERE table_name = '{{ target_table_name }}'
    {% endset %}

    {# 2. Выполняем запрос на этапе компиляции dbt #}
    {% set results = run_query(get_rules_query) %}

    {# 3. Если данные получены (execute = true), генерируем CASE WHEN #}
    {% if execute %}
        {% set rules = results.rows %}
        
        CASE 
        {% for rule in rules %}
            {% set col = rule[0] %}
            {% set r_type = rule[1] %}
            {% set r_val = rule[2] %}
            {% set err_msg = rule[3] %}

            {# Динамически подставляем логику в зависимости от типа правила #}
            {% if r_type == 'greater_than' %}
                WHEN {{ col }} <= {{ r_val }} THEN '{{ err_msg }}'
            {% elif r_type == 'less_than' %}
                WHEN {{ col }} >= {{ r_val }} THEN '{{ err_msg }}'
            {% elif r_type == 'in_list' %}
                WHEN {{ col }} NOT IN ({{ r_val }}) THEN '{{ err_msg }}'
            {% elif r_type == 'not_null' %}
                WHEN {{ col }} IS NULL THEN '{{ err_msg }}'
            {% elif r_type == 'between' %}
                {% set limits = r_val.split(',') %}
                WHEN {{ col }} < {{ limits[0] | trim }} OR {{ col }} > {{ limits[1] | trim }} THEN '{{ err_msg }}'
            {% endif %}
            
        {% endfor %}
            ELSE 'Valid'
        END
        
    {% else %}
        {# Fallback для парсера dbt, когда запрос еще не выполнен #}
        'Valid' 
    {% endif %}

{% endmacro %}