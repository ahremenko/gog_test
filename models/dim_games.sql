
{{ config(materialized='table') }}

with dim_games as (
    select distinct 
        game_id,
        game_title,
        genre,
        developer,
        release_date 
    from raw.game_metadata
)

select * from dim_games