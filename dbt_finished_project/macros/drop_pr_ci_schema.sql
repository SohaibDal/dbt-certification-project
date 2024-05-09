{%- macro drop_pr_ci_schema(target_database=target.database) %}

    {% set pr_cleanup_query %}
        select schema_name
        from {{ target_database }}.information_schema.schemata
        where
        schema_name like 'DBT_CI%'
    {% endset %}

    {% do log(pr_cleanup_query, info=TRUE) %}

    {% set drop_commands = run_query(pr_cleanup_query).columns[0].values() %}

    {% if drop_commands %}
        {% for drop_command in drop_commands %}
            {% do snowflake_utils.drop_schema(drop_command, target_database) %}
        {% endfor %}
    {% else %}
        {% do log('No schemas to drop.', True) %}
    {% endif %}

{%- endmacro %}