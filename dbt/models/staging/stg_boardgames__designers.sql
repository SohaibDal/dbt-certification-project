with

designers as (

    select * from {{ source('boardgame', 'designers') }}

),

final as (

    select
        game_id as boardgame_id,
        case
            when designers = '0' then '{{ var("unknown") }}'
            else designers
        end as designer_name

    from designers

)

select * from final