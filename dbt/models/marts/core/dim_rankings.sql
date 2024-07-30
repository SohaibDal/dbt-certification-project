with boardgames_filtered as (
    select * from {{ ref('int_boardgames__boardgames_filtered') }}
),

rankings as (
    select * from {{ ref('stg_boardgames__rankings') }}
),

dim_rankings as (
    select
        {{ dbt_utils.generate_surrogate_key(['boardgame_rank', 'rankings.boardgame_id', 'updated_at']) }} as ranking_key,
        {{ dbt_utils.generate_surrogate_key(['rankings.boardgame_id']) }} as boardgame_key,
        boardgame_rank,
        boardgame_total_reviews,
        boardgame_url,
        boardgame_thumbnail,
        updated_at,
        valid_from,
        valid_to,
        is_current

    from rankings
    where boardgame_id in (select boardgame_id from boardgames_filtered)
)

select * from dim_rankings