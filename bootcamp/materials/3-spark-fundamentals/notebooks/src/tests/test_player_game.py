from chispa.dataframe_comparer import *
from ..jobs.player_game_edges_job import do_player_games_transformation
from collections import namedtuple
game_detail = namedtuple("game_detail", "player_id game_id start_position pts team_id team_abbreviation")
#Playeredge = namedtuple("Playeredge", "subject_type object_identifier object_type edge_type properties")
Playeredge = namedtuple("Playeredge", "subject_identifier subject_type object_identifier object_type edge_type")

    
#{"start_position" : "F", "pts" : 4, "team_id" : 1610612750, "team_abbreviation" : "MIN"

def test_scd_generation(spark):
    source_data1 = [
        game_detail("111", "00000001", "F", 4, 1610612750, "MIN"),
        game_detail("123", "1", "F", 14, 1610612750, "MIN"),
        game_detail("999", "007", "G" , 24, 1610612750, "MIN")
    ]
    source_df1 = spark.createDataFrame(source_data1)

    actual_df = do_player_games_transformation(spark, source_df1)
    expected_data = [
        Playeredge("111", "player", "00000001", "game", "plays_in"),#, {"start_position" : "F", "pts" : 4, "team_id" : 1610612750, "team_abbreviation" : "MIN"}),
        Playeredge("123", "player", "1", "game", "plays_in"),#, {"start_position" : "C", "pts" : 14, "team_id" : 1610612750, "team_abbreviation" : "MIN"}),
        Playeredge("999", "player", "007",  "game", "plays_in")#, {"start_position" : "G", "pts" : 24, "team_id" : 1610612750, "team_abbreviation" : "MIN"})
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df, ignore_nullable  = True)