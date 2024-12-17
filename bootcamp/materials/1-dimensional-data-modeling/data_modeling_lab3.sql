

-- base ddls
CREATE TYPE vertex_type
    AS ENUM('player', 'team', 'game');

-- DROP TABLE vertices
CREATE TABLE vertices(
    identifier TEXT
    , type vertex_type
    , properties JSON
    , PRIMARY KEY (identifier, type)
);

CREATE TYPE edge_type AS   
    ENUM('plays_against', 'shares_team', 'plays_in', 'plays_on');

-- DROP TABLE edges
CREATE TABLE edges(
    subject_identifer TEXT
    , subject_type vertex_type
    , object_identifer TEXT
    , object_type vertex_type
    , edge_type edge_type
    , properties JSON
    , PRIMARY KEY (subject_identifer
        , subject_type
        , object_identifer
        , object_type
        , edge_type)
)

-- relationship entity pops
INSERT INTO vertices(
SELECT
    game_id AS identifier
    , 'game'::vertex_type as type,
    json_build_object(
        'pts_home', pts_home,
        'pts_away', pts_away,
        'winning_team', CASE 
            WHEN home_team_wins = 1  THEN home_team_id  
            ELSE  home_team_id
        END
    ) as properties
FROM games)

INSERT INTO vertices(WITH player_agg AS (SELECT 
    player_id AS identifier
    , MAX(player_name) as player_name
    , count(1) as number_of_games
    , SUM(pts) as total_pts
    , array_agg(distinct team_id) as teams
    FROM game_details
    GROUP BY player_id)
SELECT identifier, 'player'::vertex_type
    , json_build_object('player_name', player_name
    ,'number_of_games', number_of_games
    , 'total_pts', total_pts
    , 'teams', teams)
FROM player_agg);

INSERT INTO vertices(
WITH teams_deduped AS (
    select *, ROW_NUMBER() OVER (PARTITION BY team_id) AS row_num
    FROM teams)
    SELECT team_id as identifier
,  'team'::vertex_type as type
, json_build_object(
    'abbreviation', abbreviation
    ,'nickname', nickname
    ,'city', city
    , 'arena', arena
    , 'year_founded',yearfounded 
)
FROM teams_deduped
WHERE row_num = 1); 

--edges creation
INSERT INTO edges(
WITH games_deduped AS (
    select *, ROW_NUMBER() OVER (PARTITION BY player_id, game_id) AS row_num
    FROM game_details)
    SELECT player_id AS subject_identifier
,'player' as subject_type
, game_id as object_identifier
,'game'::vertex_type AS object_type
, 'plays_in'::edge_type as edge_type,
json_build_object(
    'start_position', start_position
    ,'pts', pts
    , 'team_id', team_id
    , 'team_abbreviation', team_abbreviation
) AS properties 
FROM games_deduped
WHERE row_num = 1
)

SELECT v.properties ->>'player_name',
MAX(e.properties ->> 'pts')
FROM vertices v
JOIN edges e
ON e.subject_identifer = v.identifier
AND e.subject_type = v.type
GROUP BY 1
ORDER BY 2 DESC

INSERT INTO edges(
WITH deduped AS (
    select *, ROW_NUMBER() OVER (PARTITION BY player_id, game_id) AS row_num
    FROM game_details),
    filtered AS (
    select *
    FROM deduped
    WHERE row_num = 1),
    aggregated AS(
    SELECT f1.player_id as subject_player_id 
    , MAX(f1.player_name) as subject_player_name 
    , f2.player_id
    , MAX(f2.player_name) as object_player_name 
    , CASE 
        WHEN f1.team_abbreviation = f2.team_abbreviation 
            THEN  'shares_team'::edge_type
            ELSE  'plays_against'::edge_type
    END as edge_type
    , COUNT(1) as num_games
    , sum(f1.pts) AS subject_points
    , sum(f2.pts) AS points
    FROM filtered f1
    JOIN filtered f2 
    ON f1.game_id = f2.game_id
    and f1.player_name <> f2.player_name
    GROUP BY f1.player_id
    , f2.player_id
    , CASE 
        WHEN f1.team_abbreviation = f2.team_abbreviation 
            THEN  'shares_team'::edge_type
            ELSE  'plays_against'::edge_type
    END )
    SELECT  
    subject_player_id as subject_identifier
    ,'player'::vertex_type as subject_type
    , player_id as object_player_name
    ,'player'::vertex_type as object_type
    ,  edge_type as edge_type
    , json_build_object(
        'num_games', num_games
        ,'subject_points', subject_points
        , 'object_points', points
    )
    FROM aggregated
    )

select v.properties->>'player_name'
    , e.object_identifer
    , CAST(e.properties->>'subject_points' AS REAL) 
    , CAST(e.properties->>'subject_points' AS REAL) / CASE WHEN CAST(v.properties->>'number_of_games' AS REAL) = 0 THEN 1
        ELSE CAST(v.properties->>'number_of_games' AS REAL) END
    , e.properties->>'subject_points'
    , e.properties->>'num_games'
from vertices v
join edges e
on v.identifier = e.subject_identifer
and v.type = subject_type
where e.object_type = 'player'::vertex_type