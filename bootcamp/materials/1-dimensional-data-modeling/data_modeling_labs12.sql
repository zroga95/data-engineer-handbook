-- Active: 1732812399151@@host.docker.internal@5432@postgres@public

SELECT * FROM public.player_seasons;



CREATE TYPE season_stats AS (
    season INTEGER,
    gp INTEGER,
    pts REAL,
    reb REAL,
    ast REAL
)

create type scoring_class AS ENUM( 'star', 'good',  'average', 'bad')

drop Table players

CREATE Table players(
    player_name TEXT,
    height TEXT,
    college TEXT,
    country TEXT,
    draft_year TEXT,
    draft_round TEXT,
    draft_number TEXT,
    season_stats season_stats[],
    years_since_last_season INTEGER, 
    scoring_class scoring_class,
    current_season INTEGER,
    is_active BOOLEAN,
    PRIMARY KEY(player_name, current_season)
)



INSERT INTO players(
WITH yesterday AS (
 SELECT * FROM players
 WHERE current_season = 2001
), 
    today As (
        SELECT * 
        FROM player_seasons
        WHERE season = 2002
    )    
SELECT
    COALESCE(t.player_name, y.player_name) AS player_name
    , COALESCE(t.height, y.height) AS height 
    , COALESCE(t.college, y.college) AS college
    , COALESCE(t.country, y.country) AS country
    , COALESCE(t.draft_year, y.draft_year) AS draft_year
    , COALESCE(t.draft_round, y.draft_round) AS draft_round
    , COALESCE(t.draft_number, y.draft_number) AS draft_number
    , CASE 
        WHEN y.season_stats IS NULL
         THEN ARRAY[ROW(
            t.season,
            t.gp,
            t.pts,
            t.reb,
            t.ast
         )::season_stats]
        WHEN t.season IS NOT NULL   
         THEN y.season_stats || ARRAY[ROW(
            t.season,
            t.gp,
            t.pts,
            t.reb,
            t.ast
         )::season_stats]
         ELSE y.season_stats  
    END AS season_stats
    , CASE 
        WHEN t.season IS NOT NULL THEN 0  
        ELSE y.years_since_last_season + 1 
    END AS years_since_last_season
    , CASE 
        WHEN t.season IS NOT NULL
        THEN
            CASE 
                WHEN t.pts > 20 THEN 'star'
                WHEN t.pts > 15 THEN 'good'
                WHEN t.pts > 10 THEN 'average'  
                ELSE  'bad'
            END::scoring_class
        ELSE y.scoring_class
    END AS scoring_class
    , COALESCE(t.season, y.current_season + 1) AS current_season
    , CASE 
        WHEN t.season IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_active
    --, COALESCE(t.draft_year, y.draft_year) AS draft_year
    --, COALESCE(t.draft_year, y.draft_year) AS draft_year
    FROM today t FULL OUTER JOIN yesterday y 
    ON t.player_name = y.player_name);

WITH unnested AS (
select player_name,
unnest(season_stats)::season_stats as season_stats 
from players
where current_season = 2001
--and player_name = 'Michael Jordan'
)
select player_name, (season_stats::season_stats).*
FROM unnested un

SELECT player_name,
    (season_stats[cardinality(season_stats)]::season_stats).pts /
     CASE 
        WHEN  (season_stats[1]::season_stats).pts = 0 THEN 1
        ELSE  (season_stats[1]::season_stats).pts
     END as latest_season
from players
where current_season = 2001
and scoring_class = 'star'

drop table  players_scd

CREATE TABLE players_scd (
    player_name TEXT,
    scoring_class scoring_class,
    is_active BOOLEAN,
    start_season INTEGER,
    end_season INTEGER,
    current_season INTEGER,
    PRIMARY KEY (player_name, start_season)
 )

-- scd type 2

INSERT INTO players_scd (
WITH with_previous AS ( SELECT player_name, current_season, scoring_class
, LAG (scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) as previous_scoring_class
, is_active
, LAG (is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) as previous_is_active
FROM players
),
    with_indicators AS (
SELECT *
, CASE 
    WHEN scoring_class <> previous_scoring_class THEN 1
    WHEN is_active <> previous_is_active THEN 1 
    ELSE  0
END AS change_indicator
FROM with_previous
),
 with_streaks as (
    SELECT *, SUM(change_indicator) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
FROM with_indicators
 )
select player_name
, scoring_class
, is_active
--, streak_identifier
, min(current_season) as start_season
, max(current_season) as end_season
, 2001 as current_season
FROM with_streaks
GROUP BY
player_name
, streak_identifier
, is_active
, scoring_class
ORDER BY player_name)

SELECT * FROM players_scd

CREATE TYPE scd_type AS (
    scoring_class scoring_class,
    is_active BOOLEAN,
    start_season INTEGER,
    end_season INTEGER
)

WITH last_season_scd AS (
    SELECT * FROM players_scd
    WHERE current_season = 2001
    AND end_season = 2001
),
    historical_season_scd AS (
            SELECT player_name
            , scoring_class
            , is_active
            , start_season
            , end_season
             FROM players_scd
    WHERE current_season = 2001
    AND end_season < 2001
    ), 
    this_season_data_scd AS (
    SELECT * FROM players
    WHERE current_season = 2002
     ),
     unchanged_records AS (
SELECT ts.player_name,
    ts.scoring_class,
    ts.is_active,
    ls.start_season,
    ts.current_season as end_season
FROM this_season_data_scd ts 
    JOIN last_season_scd ls
    ON ls.player_name = ts.player_name
    WHERE ts.scoring_class = ls.scoring_class
    AND ts.is_active = ls.is_active
     ),
changed_records AS (
    SELECT ts.player_name
    , unnest(ARRAY[
            ROW(
            ls.scoring_class,
            ls.is_active,
            ls.start_season,
            ls.end_season)::scd_type,
            ROW(
            ts.scoring_class,
            ts.is_active,
            ts.current_season,
            ts.current_season)::scd_type
    ]) as records
FROM this_season_data_scd ts 
    LEFT JOIN last_season_scd ls
    ON ls.player_name = ts.player_name
    WHERE (ts.scoring_class <> ls.scoring_class)
    OR (ts.is_active <> ls.is_active)
    OR (ls.player_name IS NULL)
),
unnested_change_records AS (
    SELECT player_name
        , (records::scd_type).scoring_class
        , (records::scd_type).is_active
        , (records::scd_type).start_season
        , (records::scd_type).end_season
    FROM changed_records
),
new_records AS (
    SELECT ts.player_name,
    ts.scoring_class,
    ts.is_active,
    ts.current_season as start_season,
    ts.current_season as end_season
    FROM this_season_data_scd ts
    LEFT JOIN last_season_scd ls 
    ON  ts.player_name = ls.player_name
    WHERE ls.player_name IS NULL
)
SELECT * FROM historical_season_scd
UNION ALL 
SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM unnested_change_records
UNION ALL
SELECT * FROM new_records
