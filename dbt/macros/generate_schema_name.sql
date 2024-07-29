{# 
    This macro enforces the use of custom schemas for all models in the project. 
    In production, dbt will write the models to the custom schema configuration. 
    In dev, dbt will only use the target schema in your profiles.yml, regardless of the custom schema provided

    Example:

    1. dbt run -m fct_billed_patient_claims --target dev
    --> analytics.dbt_dconnors.fct_billed_patient_claims

    1. dbt run -m fct_billed_patient_claims --target prod
    --> analytics.billing_analytics.fct_billed_patient_claims

#}


{% macro generate_schema_name(custom_schema_name, node) -%}

    {% if node.resource_type == 'seed' %}
        {{ custom_schema_name | trim | upper }}
    {% else %}
        {{ target.schema }}
    {% endif %}

{%- endmacro %}