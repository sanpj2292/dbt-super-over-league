with teams as (
    select * from {{ ref('Teams') }}
),
cities as (
    select * from {{ ref('cities') }}
),
seasons as (
    select * from {{ ref('seasons') }}
),
matches as (
    select * from {{ ref('matches') }}
),

stage_matches as (
    select 
        mat.id,
        (select  city_id from cities where city_name=mat.city limit 1) as venue,
        (select  season_id from seasons where season_name=mat.Season limit 1) as season,
        (select  team_id from teams  where team_name = mat.team1 limit 1) as team1,
        (select  team_id from teams  where team_name = mat.team2 limit 1) as team2,
        (select  team_id from teams  where team_name = mat.winner limit 1) as winner
    from matches mat 
)

select * from stage_matches