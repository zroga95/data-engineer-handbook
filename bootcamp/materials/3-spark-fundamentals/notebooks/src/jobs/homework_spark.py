from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, lit, broadcast, count, avg


def do_game_annotation(spark):
    #bring in all tables
    medals_df = spark.read.option("header", "true").csv("/home/iceberg/data/medals.csv");
    
    maps_df = spark.read.option("header", "true").csv("/home/iceberg/data/maps.csv").select(col("mapid"),col("description").alias("maps_description"), col("name").alias("maps_name"))#.withColumn("event_date", expr("DATE_TRUNC('day', event_time)"))
    
    
    match_details_df = spark.read.option("header", "true").csv("/home/iceberg/data/match_details.csv")#.withColumn("event_date", expr("DATE_TRUNC('day', event_time)"))
    
    
    matches_df = spark.read.option("header", "true").csv("/home/iceberg/data/matches.csv")#.withColumn("event_date", expr("DATE_TRUNC('day', event_time)"))
    
    
    medal_matches_players_df = spark.read.option("header", "true").csv("/home/iceberg/data/medals_matches_players.csv")#.withColumn("event_date", 
    
    #bucket tables
    match_details_df.write\
        .bucketBy(16, 'match_id') \
        .saveAsTable('bootcamp.MatchDetailsDfBucketed', format='parquet')
    
    matches_df.write\
        .bucketBy(16, 'match_id') \
        .saveAsTable('bootcamp.MatchesDfBucketed', format='parquet')
    
    medal_matches_players_df.write\
        .bucketBy(16, 'match_id') \
        .saveAsTable('bootcamp.MedalMatchesPlayersDfBucketed', format='parquet')
    
    match_details_df_bucketed = spark.table('bootcamp.MatchDetailsDfBucketed')
    matches_df_bucketed = spark.table('bootcamp.MatchesDfBucketed')
    medal_matches_players_df_bucketed = spark.table('bootcamp.MedalMatchesPlayersDfBucketed')
    
    #all the joins
    bucket_joined = matches_df_bucketed.join( 
        match_details_df_bucketed, 'match_id', 'left').join(
        medal_matches_players_df_bucketed, ['match_id', 'player_gamertag'], 'left')
    
    result_df1 = bucket_joined.join(broadcast(medals_df), "medal_id")
    
    full_annotated_matches = result_df1.join(broadcast(maps_df), "mapid")
    full_annotated_matches.show()
    full_annotated_matches.write.mode("overwrite").saveAsTable("bootcamp.full_annotated_matches_unsorted")
    
    #aggregates
    
    full_annotated_matches.groupby(
        "player_gamertag").agg(avg("player_total_kills").alias('mtc_cnt')).orderBy(col('mtc_cnt').desc()).show(1)
    
    sql_str="select playlist_id, count(distinct match_id) as mtc_cnt" \
    " from bootcamp.full_annotated_matches_unsorted "  \
    " group by playlist_id " \
    "order by count(distinct match_id) desc"
    spark.sql(sql_str).show(1)
    
    sql_str="select mapid, maps_name, count(distinct match_id) as mtc_cnt" \
    " from bootcamp.full_annotated_matches_unsorted "  \
    " group by mapid, maps_name " \
    "order by count(distinct match_id) desc"
    spark.sql(sql_str).show(1)
    
    sql_str="select mapid, maps_name, count(match_id) as medal_cnt" \
    " from bootcamp.full_annotated_matches_unsorted "  \
    'where classification = "KillingSpree"'\
    " group by mapid, maps_name " \
    "order by count(distinct match_id) desc"
    spark.sql(sql_str).show(1)
    
    #sort for space
    full_annotated_matches
    
    first_sort_df = full_annotated_matches.sortWithinPartitions(col("mapid"),col("playlist_id"),col("match_id"))
    
    #sorted = df.repartition(10, col("event_date")) \
    #        .sortWithinPartitions(col("event_date")) \
    #        .withColumn("event_time", col("event_time").cast("timestamp")) \
    
    first_sort_df.write.mode("overwrite").saveAsTable("bootcamp.full_annotated_matches_sorted")

    return spark.sql(sql_str).show(1)

def main():
   
    spark = SparkSession.builder \
      .master("local") \
      .appName("matches_data") \
      .getOrCreate()
    
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    
    output_df = do_game_annotation(spark)

