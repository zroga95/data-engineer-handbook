from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, lit, broadcast, count, avg, get_json_object
from pyspark.sql.types import UserDefinedType, StructField, StructType, IntegerType


# Create a DataFrame


sql_str="WITH deduped AS ( \
    SELECT *, row_number() over (PARTITION BY player_id, game_id ORDER BY game_id) AS row_num \
    FROM game_details \
) \
SELECT \
    player_id AS subject_identifier, \
    'player' as subject_type, \
    game_id AS object_identifier, \
    'game' AS object_type, \
    'plays_in' AS edge_type \
    FROM deduped \
    WHERE row_num = 1;"




def do_player_games_transformation(spark, dataframe1):
    dataframe1.createOrReplaceTempView("game_details")
    #dataframe2.createOrReplaceTempView("users_cumulated")
    #spark.sql("DROP TYPE vertex_type;")
    #spark.sql("CREATE TYPE vertex_type AS ENUM('player', 'team', 'game');")
    #spark.sql("CREATE TYPE edge_type AS \
    #ENUM ('plays_against','shares_team', 'plays_in', 'plays_on' );")

    df1 = spark.sql(sql_str)

   # df1 = df1.withColumn("properties", get_json_object(
   #     "start_position", "start_position",
   #     "pts", "pts",
   #     "team_id", "team_id",
   #     "team_abbreviation", "team_abbreviation"
   # ))

    return df1

    


def main():
    spark = SparkSession.builder \
            .master("local") \
            .appName("hw2") \
            .getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    output_df = do_usr_cum_transformation(spark, spark.table("game_details"))
    output_df.write.mode("overwrite").insertInto("edges")


from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, lit, broadcast, count, avg, to_json
from pyspark.sql.types import UserDefinedType, StructField, StructType, IntegerType


# Create a DataFrame


# sql_str="WITH deduped AS ( \
#     SELECT *, row_number() over (PARTITION BY player_id, game_id ORDER BY game_id) AS row_num \
#     FROM game_details \
# ) \
# SELECT \
#     player_id AS subject_identifier, \
#     'player' as subject_type, \
#     game_id AS object_identifier, \
#     'game' AS object_type, \
#     'plays_in' AS edge_type,  \
#     to_json(struct(\
#     'start_position', 'start_position', \
#      'pts', 'pts', \
#         'team_id', 'team_id', \
#        'team_abbreviation', 'team_abbreviation' \
#     )) as properties \
#     FROM deduped \
#     WHERE row_num = 1;"
