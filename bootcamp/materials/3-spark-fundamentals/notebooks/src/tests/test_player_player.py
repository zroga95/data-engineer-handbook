from chispa.dataframe_comparer import *
from ..jobs.player_player_edges_job import do_player_player_transformation
from collections import namedtuple
game_detail = namedtuple("game_detail", "player_id player_name game_id start_position pts team_id team_abbreviation")
#Playeredge = namedtuple("Playeredge", "subject_type object_identifier object_type edge_type properties")
Playeredge = namedtuple("Playeredge", "subject_identifier subject_type object_identifier object_type edge_type")

    
#{"start_position" : "F", "pts" : 4, "team_id" : 1610612750, "team_abbreviation" : "MIN"

def test_scd_generation(spark):
    source_data1 = [
        game_detail("111", "Jon", "1", "F", 4, 1610612750, "MIN"),
        game_detail("123", "John", "1", "F", 14, 1610612750, "MIN"),
        game_detail("999", "Jack", "007", "G" , 24, 1610612752, "WAS"),
        game_detail("999", "Jack", "1", "G" , 24, 1610612752, "WAS")
    ]
    source_df1 = spark.createDataFrame(source_data1)

    actual_df = do_player_player_transformation(spark, source_df1)
    expected_data = [
        Playeredge("999", "player", "111", "player", "plays_against"),#, {"start_position" : "F", "pts" : 4, "team_id" : 1610612750, "team_abbreviation" : "MIN"}),
        Playeredge("123", "player", "111", "player", "shares_team"),#, {"start_position" : "C", "pts" : 14, "team_id" : 1610612750, "team_abbreviation" : "MIN"}),
        Playeredge("999", "player", "123",  "player", "plays_against")#, {"start_position" : "G", "pts" : 24, "team_id" : 1610612750, "team_abbreviation" : "MIN"})
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df, ignore_nullable  = True)