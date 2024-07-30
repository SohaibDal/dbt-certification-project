with boardgames_filtered as (
    select * from {{ ref('int_boardgames__boardgames_filtered') }}
),

artists as (
    select * from {{ ref('stg_boardgames__artists') }}
),

dim_artists as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['artist_name', 'artists.boardgame_id']) }} as artist_key,
        {{ dbt_utils.generate_surrogate_key(['artists.boardgame_id']) }} as boardgame_key,
        artist_name

    from artists
    where boardgame_id in (select boardgame_id from boardgames_filtered)
)

select * from dim_artists