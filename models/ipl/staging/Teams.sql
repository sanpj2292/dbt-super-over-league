with source as (
    select * from {{ ref('teams') }}
),

stage_team as
(
    select
        ROW_NUMBER() OVER (ORDER BY team ASC) AS team_id,
        team AS team_name
    from source
)

select * from stage_team