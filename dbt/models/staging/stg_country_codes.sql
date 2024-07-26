with

country_codes as (

    select * from {{ ref('country_codes') }}

),

final as (

    select
        country_code,
        country_name

    from country_codes

)

select * from final