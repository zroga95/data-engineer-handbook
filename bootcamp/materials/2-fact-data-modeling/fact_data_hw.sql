 WITH deduped AS (
SELECT g.game_date_est
    , g.season
    , g.home_team_id
    , gd.*
    , ROW_NUMBER() OVER(PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) as row_num
FROM game_details gd
JOIN games g on gd.game_id = g.game_id
) SELECT *
FROM deduped
WHERE row_num = 1

DROP TABLE user_devices_cumulated
CREATE TABLE user_devices_cumulated(
    user_id NUMERIC
    , browser_type TEXT
    , device_activity_datelist DATE[]--<STRING, ARRAY[DATE]>
--    - or you could have `browser_type` as a column with multiple rows for each user (either way works, just be consistent!)
--`browser_type`
    ,  date DATE
    ,  PRIMARY KEY (user_id, browser_type, date)
)

INSERT INTO user_devices_cumulated (
    WITH yesterday AS (
    SELECT *
    FROM user_devices_cumulated
    WHERE date = DATE('2023-01-07') --2022-12-31
), today AS (
    SELECT 
    CAST(e.user_id AS NUMERIC)
    , COUNT(1)
    --, CAST(e.device_id AS NUMERIC)
    , d.browser_type
    , DATE(CAST(e.event_time as TIMESTAMP)) AS date_active
    FROM events e
    JOIN devices d
        ON d.device_id = e.device_id
    WHERE 
    DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-08')
    AND user_id IS NOT NULL
    GROUP BY user_id, browser_type, DATE(CAST(event_time AS TIMESTAMP))
)
SELECT
COALESCE(t.user_id, y.user_id)
, COALESCE(t.browser_type, y.browser_type)
, CASE 
    WHEN y.device_activity_datelist IS NULL
        THEN ARRAY[t.date_active]  
    WHEN t.date_active IS NULL
        THEN y.device_activity_datelist
    ELSE  
        ARRAY[t.date_active] || y.device_activity_datelist 
END AS device_activity_datelist
, COALESCE(t.date_active, y.date + INTERVAL '1 DAY') AS DATE 
FROM today t
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id
AND t.browser_type = y.browser_type)


WITH users AS (
    SELECT * FROM user_devices_cumulated
    WHERE date = DATE('2023-01-08')
), series AS(
SELECT * FROM GENERATE_SERIES(DATE('2023-01-01'), DATE('2023-01-08'), INTERVAL '1 DAY') as series_date
), place_holder_ints AS (
SELECT CASE 
    WHEN device_activity_datelist @> ARRAY[DATE(series_date)] 
        THEN CAST(POW(2, 31 - (date - DATE(series_date))) AS BIGINT) 
    ELSE  0
        END::bigint AS placeholder_int_value
, * 
FROM users CROSS JOIN series
--WHERE user_id = 444502572952128450
)
SELECT
    user_id
    , browser_type
    , device_activity_datelist
    , CAST(SUM(placeholder_int_value)AS BIGINT)::bigint::bit(32) AS datelist_int
    FROM place_holder_ints
    GROUP BY user_id, browser_type, device_activity_datelist



DROP TABLE hosts_cumulated;
CREATE TABLE hosts_cumulated(
    host TEXT
    , host_activity_datelist DATE[]
    ,  date DATE
    ,  PRIMARY KEY (host, date)
)



INSERT INTO hosts_cumulated (
    WITH yesterday AS (
    SELECT *
    FROM hosts_cumulated
    WHERE date = DATE('2023-01-09') --2022-12-31 --2023-01-01
), today AS (
    SELECT 
    e.host
    , DATE(CAST(e.event_time as TIMESTAMP)) AS date_active
    FROM events e
    WHERE 
    DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-10')
    AND host IS NOT NULL
    GROUP BY host, DATE(CAST(event_time AS TIMESTAMP))
)
SELECT
COALESCE(t.host, y.host)
, CASE 
    WHEN y.host_activity_datelist IS NULL
        THEN ARRAY[t.date_active]  
    WHEN t.date_active IS NULL
        THEN y.host_activity_datelist
    ELSE  
        ARRAY[t.date_active] || y.host_activity_datelist 
END AS host_activity_datelist
, COALESCE(t.date_active, y.date + INTERVAL '1 DAY') AS DATE 
FROM today t
FULL OUTER JOIN yesterday y
ON t.host = y.host
)


DROP TABLE host_activity_reduced;
CREATE TABLE host_activity_reduced(
    month date, 
    host TEXT
    , hit_array INTEGER[]
    , unique_visitors_array INTEGER[]-- 
--    - or you could have `browser_type` as a column with multiple rows for each user (either way works, just be consistent!)
--`browser_type`
    ,  PRIMARY KEY (host, month)
)

INSERT INTO host_activity_reduced
WITH daily_aggregate AS (
    SELECT
        host 
        , DATE(event_time) AS date
        , COUNT(1) AS num_hits
        , count(distinct user_id) as num_distinct_user
    FROM events
    WHERE DATE(event_time) = DATE('2023-01-04') --2022-12-31 --2023-01-01
    AND user_id IS NOT NULL
    GROUP BY host, DATE(event_time)
), yesterday_array AS (
    SELECT * FROM host_activity_reduced
    WHERE  month = DATE('2023-01-01')
)
SELECT
    COALESCE(ya.month, DATE_TRUNC('month', da.date)) AS month
    , COALESCE(da.host, ya.host) AS host
-- add a date in da
    , CASE 
        WHEN ya.hit_array IS NOT NULL
            THEN ya.hit_array || ARRAY[COALESCE(da.num_hits, 0)]
        WHEN ya.hit_array IS NULL
            THEN array_fill(0, ARRAY[COALESCE(date - CAST(DATE_TRUNC('month', date)AS DATE), 0)])  || ARRAY[COALESCE(da.num_hits, 0)]     
    END AS hit_array
    , CASE 
        WHEN ya.unique_visitors_array IS NOT NULL --num_distinct_user
            THEN ya.unique_visitors_array || ARRAY[COALESCE(da.num_distinct_user, 0)]
        WHEN ya.unique_visitors_array IS NULL
            THEN array_fill(0, ARRAY[COALESCE(date - CAST(DATE_TRUNC('month', date)AS DATE), 0)])  || ARRAY[COALESCE(da.num_distinct_user, 0)]     
    END AS unique_visitors_array
     FROM daily_aggregate da
    FULL OUTER JOIN yesterday_array ya
    ON da.host = ya.host
    ON CONFLICT (host, month)
    DO
        UPDATE SET hit_array = EXCLUDED.hit_array
        , unique_visitors_array = EXCLUDED.unique_visitors_array;

select * from host_activitcd ..ADDcdy_reduced
