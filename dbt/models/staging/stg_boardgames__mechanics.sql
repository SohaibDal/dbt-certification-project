
with

mechanics as (

    select * from {{ source('boardgame', 'mechanics') }}

),

final as (

    select
        game_id as boardgame_id,
        case
            when mechanics = '0' then '{{ var("unknown") }}'
            else mechanics
        end as mechanic_name

    from mechanics

)

select * from final
