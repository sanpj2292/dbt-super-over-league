with source as (
    SELECT DISTINCT city FROM {{ ref('matches') }}
),

stage_cities as (
    SELECT
        ROW_NUMBER() OVER (ORDER BY city ASC) as city_id,
        city as city_name,
        NOW() as created_date
    FROM source
)

SELECT * FROM stage_cities