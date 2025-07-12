
{{ config(materialized='table') }}

with dim_games as (
    select distinct 
        game_id,
        game_title,
        genre,
        developer,
        release_date 
    from {{ ref("game_metadata") }}
)

select * from dim_games