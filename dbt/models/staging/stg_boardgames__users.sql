with

users as (

    select * from {{ source('boardgame', 'users') }}

),

final as (

    select
        users as user_username,
        url as user_url,
        case
            when trim(country) is NULL then '{{ var("unknown") }}'
            when trim(country) = 'NA' then '{{ var("unknown") }}'
            when country = '
'   then '{{ var("unknown") }}' 
            when len(country) = 2 then '{{ var("unknown") }}'
            else trim(country)
        end as country_name

    from users

)

select * from final