with source as (
    select * from {{ ref('players') }}
),

stage_players as (
    select 
        ROW_NUMBER() OVER (ORDER BY Player_Name ASC) AS player_id,
        Player_Name AS player_name,
        CAST(DOB AS Date) AS dob,
        Batting_Hand as batting_hand,
        Bowling_Skill as bowling_skill,
        Country as country,
        NOW() AS created_date
    from source
)

select * from stage_players