{% macro clean_stale_models(database=target.database, days=30, dry_run=False) %}

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
        where table_schema not in ('DBT_PROD', 'SEEDS', 'RAW', 'SNAPSHOTS')
        and last_altered <= dateadd('day', -{{ days }}, current_date())
    {% endset %}

    {% set drop_schema_commands = [] %}

    {% set schema_drop_queries %}
        select distinct
            'DROP SCHEMA IF EXISTS "' || table_schema || '";' as drop_schema_command
        from "{{ database }}".information_schema.tables
        where table_schema not in ('DBT_PROD', 'SEEDS', 'RAW', 'SNAPSHOTS')
        and last_altered <= dateadd('day', -{{ days }}, current_date())
    {% endset %}

    {% do log('\nGenerating cleanup queries...\n', info=True) %}

    {% set drop_commands = run_query(get_drop_commands_query).columns[1].values() %}
    {% set schema_drop_commands = run_query(schema_drop_queries).columns[0].values() %}

    {% for drop_command in drop_commands %}
        {% do log(drop_command, True) %}
        {% if dry_run == 'false' %}
            {% do run_query(drop_command) %}
        {% endif %}
    {% endfor %}

    {% for schema_drop_command in schema_drop_commands %}
        {% do log(schema_drop_command, True) %}
        {% if dry_run == 'false' %}
            {% do run_query(schema_drop_command) %}
        {% endif %}
    {% endfor %}

{% endmacro %}