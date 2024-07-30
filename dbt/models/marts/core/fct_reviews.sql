with reviews as (
    select * from {{ ref('stg_boardgames__reviews') }}
),

dim_users as (
    select * from {{ ref('dim_users') }}
),

dim_boardgames as (
    select * from {{ ref('dim_boardgames') }}
),

dim_reviews as (
    select
        {{ dbt_utils.generate_surrogate_key(['dim_users.user_key', 'dim_boardgames.boardgame_key', 'review_rating']) }} as review_key,
        dim_users.user_key,
        dim_boardgames.boardgame_key,
        review_rating

    from reviews
    inner join dim_users on dim_users.user_username = reviews.review_username
    inner join dim_boardgames on dim_boardgames.boardgame_id = reviews.boardgame_id
)

select * from dim_reviews