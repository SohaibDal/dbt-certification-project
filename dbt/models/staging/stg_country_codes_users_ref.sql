with

country_codes_users_ref as (

    select * from {{ ref('country_codes_users_ref') }}

),

final as (

    select
        country_code,
        country_name

    from country_codes_users_ref

)

select * from final