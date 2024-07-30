with boardgames_filtered as (
    select * from {{ ref('int_boardgames__boardgames_filtered') }}
),

categories as (
    select * from {{ ref('stg_boardgames__categories') }}
),

dim_categories as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['category_name', 'categories.boardgame_id']) }} as category_key,
        {{ dbt_utils.generate_surrogate_key(['categories.boardgame_id']) }} as boardgame_key,
        category_name

    from categories
    where boardgame_id in (select boardgame_id from boardgames_filtered)
)

select * from dim_categories