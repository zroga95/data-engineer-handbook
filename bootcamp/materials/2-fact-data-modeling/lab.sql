INSERT INTO fct_game_details
(
    WITH deduped AS (
SELECT g.game_date_est
    , g.season
    , g.home_team_id
    , gd.*
    , ROW_NUMBER() OVER(PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) as row_num
FROM game_details gd
JOIN games g on gd.game_id = g.game_id
)
SELECT game_date_est as dim_game_date
, season AS dim_season
, team_id AS dim_team_id
, player_id as dim_player_id
, player_name as dim_player_name
, team_id = home_team_id AS dim_is_playing_at_home
, start_position AS dim_start_positiom
, COALESCE(POSITION('DNP' in comment),0) > 0 as dim_did_not_play
, COALESCE(POSITION('DND' in comment),0) > 0 as dim_did_not_dress
, COALESCE(POSITION('NWT' in comment),0) > 0 as dim_not_with_team
, CAST(SPLIT_PART(min, ':', 1) AS REAL) +
    CAST(SPLIT_PART(min,':', 2) AS REAL)/60 AS dim_minutes
, fgm AS m_fgm 
, fga AS m_fgm
, fg3m AS m_fg3m
, fg3a AS m_fg3a
, ftm AS m_ftm
, fta AS m_fta
, oreb AS m_oreb
, dreb AS m_dreb
, reb AS m_reb
, ast AS m_ast
, stl AS m_stl
, blk AS m_blk 
, "TO" AS m_turnovers
, pf AS m_pf
, pts AS m_pts
, plus_minus AS m_plus_minus
 FROM deduped
WHERE row_num = 1
ORDER BY row_num DESC)

--DROP TABLE fct_game_details

CREATE TABLE fct_game_details(
dim_game_date DATE
, dim_season INTEGER
, dim_team_id INTEGER
, dim_player_id INTEGER
, dim_player_name TEXT
, dim_is_playing_at_home BOOLEAN
, dim_start_position TEXT
, dim_did_not_play BOOLEAN
, dim_did_not_dress BOOLEAN
, dim_not_with_team BOOLEAN
, m_minutes REAL
, m_fgm INTEGER 
, m_fga INTEGER
, m_fg3m INTEGER
, m_fg3a INTEGER
, m_ftm INTEGER
, m_fta INTEGER
, m_oreb INTEGER
, m_dreb INTEGER
, m_reb INTEGER
, m_ast INTEGER
, m_stl INTEGER
, m_blk INTEGER
, m_turnovers INTEGER
, m_pf INTEGER
, m_pts INTEGER
, m_plus_minus INTEGER
, PRIMARY KEY (dim_game_date, dim_player_id, dim_team_id)
)


SELECT t.*, gd.* FROM fct_game_details gd JOIN teams t
ON t.team_id = gd.dim_team_id

select dim_player_name
, count(1) as num_games
, COUNT(CASE 
    WHEN dim_not_with_team THEN 1 END) as bailed_num
 ,  CAST(COUNT(CASE 
    WHEN dim_not_with_team THEN 1 END) AS REAL)/ count(1) as perc_bailed
from fct_game_details
group by 1
order by 4


-- lab 2

select * from events

DROP TABLE users_cumulated

CREATE TABLE users_cumulated(
    user_id TEXT
--list of dates where user active, current date for user
    , date_active DATE[]
    , date DATE
    , PRIMARY KEY (user_id, date)
)

INSERT INTO users_cumulated(
WITH yesterday AS (
    SELECT *
    FROM users_cumulated
    WHERE date = DATE('2023-01-30')
), today AS (
    SELECT 
    CAST(user_id AS TEXT) 
    , COUNT(1)
    , DATE(CAST(event_time as TIMESTAMP)) AS date_active
    FROM events
    WHERE 
    DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
    AND user_id IS NOT NULL
    GROUP BY user_id, DATE(CAST(event_time AS TIMESTAMP))
)
SELECT
COALESCE(t.user_id, y.user_id)
, CASE 
    WHEN y.date_active IS NULL
        THEN ARRAY[t.date_active]  
    WHEN t.date_active IS NULL
        THEN y.date_active
    ELSE  
        ARRAY[t.date_active] || y.date_active 
END AS date_active
, COALESCE(t.date_active, y.date + INTERVAL '1 DAY') AS DATE 
FROM today t
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id
)

WITH users AS (
    SELECT * FROM users_cumulated
    WHERE date = DATE('2023-01-31')
), series AS(
SELECT * FROM GENERATE_SERIES(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 DAY') as series_date
), place_holder_ints AS (
SELECT CASE 
    WHEN date_active @> ARRAY[DATE(series_date)] 
        THEN CAST(POW(2, 32 - (date - DATE(series_date))) AS BIGINT) 
    ELSE  0
        END::bigint AS placeholder_int_value
, * 
FROM users CROSS JOIN series
--where user_id = '137925124111668560'
)
SELECT 
    user_id
    , CAST(SUM(placeholder_int_value)AS BIGINT)::bigint::bit(32)
    , bit_count(CAST(SUM(placeholder_int_value)AS BIGINT)::bigint::bit(32)) > 0 AS dim_is_monthly_active
    , bit_count(CAST('11111110000000000000000000000000' AS BIT(32)) &
        CAST(SUM(placeholder_int_value) AS BIGINT)::bigint::bit(32)) > 0 AS dim_is_weekly_active
    FROM place_holder_ints
    GROUP BY user_id

--lab3

--DELETE FROM array_metrics

CREATE TABLE array_metrics (
    user_id NUMERIC
    , month_start DATE
    , metric_name TEXT
    , metric_array REAL[]
    , PRIMARY KEY (user_id, month_start, metric_name)
)

 
INSERT INTO array_metrics
WITH daily_aggregate AS (
    SELECT
        user_id
        , DATE(event_time) AS date
        , COUNT(1) AS num_site_hits
    FROM events
    WHERE DATE(event_time) = DATE('2023-01-04')
    AND user_id IS NOT NULL
    GROUP BY user_id, DATE(event_time )
), yesterday_array AS (
    SELECT * FROM array_metrics
    WHERE  month_start = DATE('2023-01-01')
)
SELECT
    COALESCE(da.user_id, ya.user_id) AS user_id
    , COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start -- add a date in da
    , 'site_hits' AS metric_name
    , CASE 
        WHEN ya.metric_array IS NOT NULL
            THEN ya.metric_array || ARRAY[COALESCE(da.num_site_hits, 0)]
        WHEN ya.metric_array IS NULL
            THEN array_fill(0, ARRAY[COALESCE(date - CAST(DATE_TRUNC('month', date)AS DATE), 0)])  || ARRAY[COALESCE(da.num_site_hits, 0)]     
    END AS metric_array
     FROM daily_aggregate da
    FULL OUTER JOIN yesterday_array ya
    ON da.user_id = ya.user_id
    ON CONFLICT (user_id, month_start, metric_name)
    DO
        UPDATE SET metric_array = EXCLUDED.metric_array;--email = EXCLUDED.email || ';' || customers.email
    
    
SELECT cardinality(metric_array), count(1)
 FROM array_metrics
GROUP BY 1

WITH agg AS (SELECT metric_name, month_start, ARRAY[SUM(metric_array[1])
                    , SUM(metric_array[2])
                    , SUM(metric_array[3])
                    , SUM(metric_array[4])
                    ] as summed_array
FROM array_metrics
GROUP BY metric_name, month_start
)
SELECT metric_name
, month_start + CAST(CAST(index - 1 AS TEXT) || 'day' AS INTERVAL)
, elem as value
 FROM agg
    CROSS join unnest(agg.summed_array)
        WITH ORDINALITY AS a(elem ,index)