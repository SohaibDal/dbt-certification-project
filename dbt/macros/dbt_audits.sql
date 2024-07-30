{% macro audit_log(run_activity) %}

{% if run_activity in ['model_start', 'model_end']%}
    insert into {{ this.database }}.{{ this.schema }}.dbt_audit 
    values ('{{ invocation_id }}', '{{ this.name }}', '{{ run_activity }}', sysdate());

{% else %}

    insert into {{ target.database }}.{{ target.schema }}.dbt_audit 
    values ('{{ invocation_id }}', '{{ this.name }}', '{{ run_activity }}', sysdate());

{% endif %}

{% endmacro %}


{% macro audit_prep() %}
create table if not exists {{ target.database }}.{{ target.schema }}.dbt_audit
(
    run_id text,
    run_object text,
    run_activity text,
    created_at timestamp_ntz
);

{% endmacro %}