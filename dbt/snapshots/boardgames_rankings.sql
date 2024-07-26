{% snapshot rankings %}

{{
    config(
      target_database='boardgame',
      target_schema='dbt_sansari',
      unique_key='id',

      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select 
    ID,
    "Name",
    "Year",
    "Rank",
    "Average",
    "Bayes average",
    "Users rated",
    URL,
    "Thumbnail",
    "updated_at" as updated_at

from {{ source('boardgame', 'rankings') }}

{% endsnapshot %}