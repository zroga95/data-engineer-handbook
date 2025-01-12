from chispa.dataframe_comparer import *

from ..jobs.team_vertices_job import do_team_vertext_transformation
from collections import namedtuple

TeamVertex = namedtuple("TeamVertex", "identifier type properties")
Team = namedtuple("Team", "team_id abbreviation nickname city arena yearfounded")

def test_vertex_generation(spark):
    input_data = [
        Team(1, "GSW", "Warriors", "SF", "Chase Center", 1900),
        Team(1, "GSW", " Bad Warriors", "SF", "Chase Center", 1900)
    ]

    input_dataframe = spark.createDataFrame(input_data)

    actual_df = do_team_vertext_transformation(spark, input_dataframe) 

    expected_values = [
        TeamVertex(
            identifier = 1,
            type = 'team',
            properties={
                'abbreviation': 'GSW',
                'nickname': 'Warriors',
                'city': 'SF',
                'arena': 'Chase Center',
                'year_founded': '1900'
            }
        )
    ]

    expected_df = spark.createDataFrame(expected_values)

    assert_df_equality(actual_df, expected_df, ignore_nullable  = True )
