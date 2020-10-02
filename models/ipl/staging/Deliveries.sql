with deliveries as (
    select * from {{ ref('deliveries') }}
),
src_players as (
    select * from {{ source('ipl_postgres', 'Players') }}
),
src_teams as (
    select * from {{ source('ipl_postgres', 'Teams') }}
)
select 
    ROW_NUMBER() OVER (ORDER BY d.match_id ASC) as delivery_id,
    d.match_id,
    d.inning,
    bat_t.team_id as batting_team_id,
    bow_t.team_id as bowling_team_id,
    d.over,
    d.ball,
    batp.player_id as batsman_id,
    nsp.player_id as non_striker_id,
    bowp.player_id as bowler_id,
    cast(d.is_super_over as BIT) as is_super_over,
    cast(d.wide_runs as INT) as wide_runs,
    cast(d.bye_runs as INT) as bye_runs,
    cast(d.legbye_runs as INT) as legbye_runs,
    cast(d.noball_runs as INT) as noball_runs,
    cast(d.penalty_runs as INT) as penalty_runs,
    cast(d.batsman_runs as INT) as batsman_runs,
    cast(d.extra_runs as INT) as extra_runs,
    cast(d.total_runs as INT) as total_runs,
    disp.player_id as player_dismissed_id,
    d.dismissal_kind,
    fp.player_id as fielder_id
from deliveries d
left join src_players batp on batp.player_name = d.batsman
left join src_players bowp on bowp.player_name = d.bowler
left join src_players nsp on nsp.player_name = d.non_striker
left join src_players disp on disp.player_name = d.player_dismissed
left join src_players fp on fp.player_name = d.fielder
left join src_teams bat_t on bat_t.team_name = d.batting_team
left join src_teams bow_t on bow_t.team_name = d.bowling_team