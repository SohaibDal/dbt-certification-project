with

artists as (

    select * from {{ source('boardgame', 'artists') }}

),

final as (

    select
        game_id as boardgame_id,
        case
            when artists = '0' then '{{ var("unknown") }}'
            else artists
        end as artist_name

    from artists

)

select * from final