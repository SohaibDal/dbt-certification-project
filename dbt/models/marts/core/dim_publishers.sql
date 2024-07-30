with boardgames_filtered as (
    select * from {{ ref('int_boardgames__boardgames_filtered') }}
),

publishers as (
    select * from {{ ref('stg_boardgames__publishers') }}
),

dim_publishers as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['publisher_name', 'publishers.boardgame_id']) }} as publisher_key,
        {{ dbt_utils.generate_surrogate_key(['publishers.boardgame_id']) }} as boardgame_key,
        publishers.boardgame_id,
        publisher_name

    from publishers
    where boardgame_id in (select boardgame_id from boardgames_filtered)
)

select * from dim_publishers