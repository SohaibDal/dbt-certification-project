with  __dbt__cte__int_boardgames__boardgames_filtered as (
with
    
boardgames as (
    select * from BOARDGAME.dbt_prod.stg_boardgames__boardgames
),

reviews as (
    select * from BOARDGAME.dbt_prod.stg_boardgames__reviews
),

boardgames_filtered as (
    select
        *
    from boardgames
    where 
        boardgame_id in (select boardgame_id from reviews)
        and boardgame_type = 'boardgame'
        and boardgame_avg_rating <> '-1'
        and boardgame_avg_weight <> '-1'
)

select * from boardgames_filtered
), boardgames_filtered as (
    select * from __dbt__cte__int_boardgames__boardgames_filtered
),

artists as (
    select * from BOARDGAME.dbt_prod.stg_boardgames__artists
),

dim_artists as (
    select distinct
        md5(cast(coalesce(cast(artist_name as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(artists.boardgame_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as artist_key,
        md5(cast(coalesce(cast(artists.boardgame_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as boardgame_key,
        artist_name

    from artists
    where boardgame_id in (select boardgame_id from boardgames_filtered)
)

select * from dim_artists