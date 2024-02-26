{% macro clean_stale_models(database=target.database, days=30, dry_run=True) %}

    {% set get_drop_commands_query %}
        select
            case 
                when table_type = 'VIEW'
                    then 'VIEW'
                else 
                    'TABLE'
            end as drop_type, 
            'DROP ' || drop_type || ' "' || table_catalog || '"."' || table_schema || '"."' || table_name || '";' as drop_command
        from "{{ database }}".information_schema.tables 
        where table_schema ilike 'DBT_%'
        and table_schema <> 'DBT_PROD'  -- Exclude the 'DBT_PROD' schema
        and last_altered <= dateadd('day', -{{ days }}, current_date())
    {% endset %}

    {{ log('\nGenerating cleanup queries...\n', info=True) }}
    {% set results = run_query(get_drop_commands_query) %}

    {% if execute %}
        {% for row in results %}
            {% set query = row[1] %}

            {% if dry_run %}
                {{ log(query, info=True) }}
            {% else %}
                {{ log('Dropping object with command: ' ~ query, info=True) }}
                {% do run_query(query) %} 
            {% endif %}
        {% endfor %}
    {% endif %}

{% endmacro %}