{{
    config(
        materialized='incremental',
        unique_key='ranking_key',
        pre_hook = [
            '{% if is_incremental() %} DELETE FROM {{this}} WHERE date(updated_at) <> current_date()  {% endif %}'
        ]
    )
}}

with dim_rankings as (
    select * from {{ ref('dim_rankings') }}
)

select * from dim_rankings

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records arriving later on the same day as the last run of this model)
  where updated_at > (select max(updated_at) from {{ this }})

{% endif %}