{% test accepted_values_between_1_10(model, column_name) %}

with validation as (
    select
        {{ column_name }} as accepted_values_between_1_10
    from {{ model }}
),

validation_errors as (
    select
        accepted_values_between_1_10
    from validation
    where accepted_values_between_1_10 not between 1 and 10
)

select *
from validation_errors

{% endtest %}