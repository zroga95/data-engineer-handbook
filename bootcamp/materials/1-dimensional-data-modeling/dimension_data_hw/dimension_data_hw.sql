
--DROP TYPE quality_class;

CREATE TYPE quality_class AS ENUM( 'star', 'good',  'average', 'bad');

--DROP TYPE film;
CREATE TYPE film AS (
    film TEXT
    , votes INTEGER
    , rating FLOAT
    , filmid TEXT
);

--Drop Table actors

CREATE Table actors(
    actor TEXT
    , actorid TEXT
    , films film[]
    , quality_class quality_class
    , current_year INTEGER
    , is_active BOOLEAN
    , PRIMARY KEY(actor, current_year)
)


INSERT INTO actors(
 WITH last_year AS (
 SELECT * FROM actors
 WHERE current_year = 1973
) 
    , current_year AS (
        SELECT actor 
, actorid
, ARRAY_AGG(ROW(film
, votes
, rating
, filmid)::film) AS films
, AVG(rating) AS avg_rating
, MAX(year) AS current_year
FROM actor_films 
WHERE year = 1974
GROUP BY actor, actorid
    )    
SELECT
    COALESCE(c.actor, l.actor) AS actor
    , COALESCE(c.actorid, l.actorid) AS actorid 
    , CASE 
        WHEN l.films IS NULL
         THEN c.films
        WHEN c.films IS NOT NULL   
         THEN c.films || l.films
        ELSE l.films  
    END AS films
    , CASE 
        WHEN c.current_year IS NOT NULL
        THEN
            CASE 
                WHEN c.avg_rating > 8 THEN 'star'
                WHEN c.avg_rating > 7 THEN 'good'
                WHEN c.avg_rating > 6 THEN 'average'  
                ELSE  'bad'
            END::quality_class
        ELSE l.quality_class
    END AS quality_class
    , COALESCE(c.current_year, l.current_year + 1) AS current_year
    , CASE 
        WHEN c.current_year IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_active
    FROM current_year c FULL OUTER JOIN last_year l 
    ON l.actor = c.actor
    );


--DROP TABLE actors_history_scd

CREATE TABLE actors_history_scd(
    actor TEXT
    , actorid TEXT
    , start_date INTEGER
    , end_date INTEGER
    , quality_class quality_class
    , is_active BOOLEAN
    , current_year INTEGER
    , PRIMARY KEY(actor, actorid, start_date)
)


INSERT INTO actors_history_scd (
WITH with_previous AS (
    SELECT actor, actorid, current_year, quality_class
, LAG (quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_year) as previous_quality_class
, is_active
, LAG (is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year) as previous_is_active
FROM actors
),
    with_indicators AS (
SELECT *
, CASE 
    WHEN quality_class <> previous_quality_class THEN 1
    WHEN is_active <> previous_is_active THEN 1 
    ELSE  0
END AS change_indicator
FROM with_previous
),
 with_streaks as (
    SELECT *, SUM(change_indicator) OVER (PARTITION BY actorid ORDER BY current_year) AS streak_identifier
FROM with_indicators
 )
select actor
, actorid
, min(current_year) as start_date
, max(current_year) as end_date
, quality_class
, is_active
, 1973 as current_season
FROM with_streaks
GROUP BY
actor
, actorid
, streak_identifier
, is_active
, quality_class
ORDER BY actor)


CREATE TYPE scd_type AS (
    quality_class quality_class,
    is_active BOOLEAN,
    start_date INTEGER,
    end_date INTEGER
)


WITH last_year_scd AS (
    SELECT * FROM actors_history_scd
    WHERE current_year = 1973
    AND end_date = 1973
)
    , historical_scd AS (
            SELECT actor
            , actorid
            , start_date
            , end_date
            , quality_class
            , is_active
             FROM actors_history_scd
    WHERE current_year = 1973
    AND end_date < 1973
    ) 
    , this_year_data_scd AS (
    SELECT * FROM actors
    WHERE current_year = 1974
     )
     , unchanged_records AS (
SELECT ts.actor
    , ts.actorid
    , ls.start_date
    , ts.current_year as end_date
    , ts.quality_class
    , ts.is_active
FROM this_year_data_scd ts 
    JOIN last_year_scd ls
    ON ls.actorid = ts.actorid
    AND ls.actorid = ts.actorid
    WHERE ts.quality_class = ls.quality_class
    AND ts.is_active = ls.is_active
     )
, changed_records AS (
    SELECT ts.actor
    , ts.actorid
    , unnest(ARRAY[
            ROW(
            ls.quality_class,
            ls.is_active,
            ls.start_date,
            ls.end_date)::scd_type,
            ROW(
            ts.quality_class,
            ts.is_active,
            ts.current_year,
            ts.current_year)::scd_type
    ]) as records
FROM this_year_data_scd ts 
    LEFT JOIN last_year_scd ls
    ON ls.actor = ts.actor
    AND ls.actorid = ts.actorid
    WHERE (ts.quality_class <> ls.quality_class)
    OR (ts.is_active <> ls.is_active)
    OR (ls.actorid IS NULL)
    )
, unnested_change_records AS (
    SELECT actor, actorid
        , (records::scd_type).start_date
        , (records::scd_type).end_date, (records::scd_type).quality_class
        , (records::scd_type).is_active
    FROM changed_records
    )
, new_records AS (
    SELECT ts.actor
    , ts.actorid
    , ts.current_year as start_date
    , ts.current_year as end_date
    , ts.quality_class
    , ts.is_active
    FROM this_year_data_scd ts
    LEFT JOIN last_year_scd ls 
        ON ls.actor = ts.actor
    AND ls.actorid = ts.actorid
    WHERE ls.actor IS NULL
)
SELECT *, 1974 as current_year
FROM (
SELECT * FROM historical_scd
UNION ALL 
SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM unnested_change_records
UNION ALL
SELECT * FROM new_records) a
