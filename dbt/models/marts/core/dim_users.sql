with users_countries_joined as (
    select * from {{ ref('int_users_countries_joined') }}
),

dim_users as (
    select
        {{ dbt_utils.generate_surrogate_key(['user_username', 'user_url', 'country_code']) }} as user_key,
        user_username,
        user_url,
        country_name

    from users_countries_joined
)

select * from dim_users