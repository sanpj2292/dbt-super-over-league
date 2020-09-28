with source as (
    SELECT DISTINCT Season FROM {{ ref('matches') }}
),

stage_seasons as (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY Season ASC) as season_id,
        Season as season_name,
        NOW() as created_date
    FROM source
)

select * from stage_seasons