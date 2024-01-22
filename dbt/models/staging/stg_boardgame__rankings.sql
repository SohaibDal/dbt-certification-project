with

source as (
    select * from {{ ref('rankings_snapshot') }}
),

transformed as (
    select
        Id as ranking_id,
        "Name" as boardgame_name,
        "Year" as boardgame_year_published,
        "Rank" as boardgame_rank,
        
        cast(
            case 
                when "Average" < 1 then 1
                else "Average"
            end as float
        ) as boardgame_avg_rating,

        cast(
            case
                when "Bayes average" < 1 then 1
                else "Bayes average"
            end as float
        ) as boardgame_bayes_avg_rating,

        "Users rated" as boardgame_total_reviews,
        concat('https://boardgamegeek.com', url) as boardgame_url,
        "Thumbnail" as boardgame_thumbnail,

        case
            when dbt_valid_to is NULL then true
            else false
        end as is_current,

        "updated_at",
        dbt_valid_from as valid_from,
        coalesce(
            dbt_valid_to,
            cast( '{{ var("the_distant_future") }}' as timestamp)
        ) as valid_to
    from source
)

select * from transformed