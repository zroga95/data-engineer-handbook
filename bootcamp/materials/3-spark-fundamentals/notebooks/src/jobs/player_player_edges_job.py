from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, lit, broadcast, count, avg, get_json_object
from pyspark.sql.types import UserDefinedType, StructField, StructType, IntegerType


# Create a DataFrame


sql_str="WITH deduped AS ( \
    SELECT *, row_number() over (PARTITION BY player_id, game_id ORDER BY game_id) AS row_num \
    FROM game_details \
), \
filtered AS ( \
         SELECT * FROM deduped \
         WHERE row_num = 1 \
     ), \
aggregated AS ( \
          SELECT \
           f1.player_id AS subject_identifier, \
            f1.player_name AS subject_name, \
           f2.player_id AS object_identifier, \
           f2.player_name AS object_name, \
           CASE WHEN f1.team_abbreviation = f2.team_abbreviation \
                THEN 'shares_team' \
            ELSE 'plays_against' \
            END AS edge_type, \
            COUNT(1) AS num_games, \
            SUM(f1.pts) AS left_points, \
            SUM(f2.pts) as right_points \
        FROM filtered f1 \
            JOIN filtered f2 \
            ON f1.game_id = f2.game_id \
            AND f1.player_name <> f2.player_name \
        WHERE f1.player_id > f2.player_id \
        GROUP BY \
                f1.player_id, \
            f1.player_name, \
           f2.player_id, \
           f2.player_name, \
           CASE WHEN f1.team_abbreviation = f2.team_abbreviation \
                THEN  'shares_team' \
            ELSE 'plays_against' \
            END \
     ) \
    SELECT \
    subject_identifier AS subject_identifier, \
    'player' as subject_type, \
    object_identifier AS object_identifier, \
    'player' AS object_type, \
    edge_type \
    FROM aggregated;"




def do_player_player_transformation(spark, dataframe1):
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
