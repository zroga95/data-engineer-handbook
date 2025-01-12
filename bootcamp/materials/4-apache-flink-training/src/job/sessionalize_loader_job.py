import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.udf import AggregateFunction
from pyflink.table.window import Tumble, Session
from pyflink.table import expressions as expr
from pyflink.table.types import DataTypes
from pyflink.table import DataTypes
from pyflink.table.udf import udf


def create_sessioned_src(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = 'process_events_kafka'
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    
    src_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            host VARCHAR,
            event_time VARCHAR,
            url VARCHAR,
            window_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '5' MINUTE
        ) WITH (
             'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(src_ddl)
    return table_name


def create_sessioned_events_sink_postgres(t_env):
    table_name = 'sink_table'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            host VARCHAR,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            url_list VARCHAR
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'


        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name





def log_aggregation():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    print('got streaming environment')
    env.enable_checkpointing(10)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    #t_env.create_temporary_function("multiset_to_string", multiset_to_string)

    try:
        # Create Kafka table
        source_table = create_sessioned_src(t_env)

        print('first_table')

        aggregated_table = create_sessioned_events_sink_postgres(t_env)

        # t_env.from_path(source_table).window(
        #     Tumble.over(lit(1).minutes).on(col("window_timestamp")).alias("w") #Session.with_gap(lit(1).minutes)
        # ).group_by(
        #     col("w"),
        #     col("ip"), 
        #     col("host"),        
        # ) \
        #     .select(#"ip, host, SESSION_START(w), SESSION_END(w), ARRAY_AGG(url)")\
        #     col("ip"), 
        #     col("host"),
        #     col("w").start.alias("session_start"),
        #     col("w").start.alias("session_end"),  
        #     col("url").max.alias("url_list")#col("url").collect.cast("STRING")#expr.array_agg(col("url")).alias("url_list")
        # ) \
        #     .execute_insert(aggregated_table)
        #     #.wait()##

        t_env.execute_sql(
            f"""
                    INSERT INTO {aggregated_table}
                    SELECT
                        ip
                        , host
                        , SESSION_START(window_timestamp, INTERVAL '5' MINUTE) AS session_start
                        , SESSION_END(window_timestamp, INTERVAL '5' MINUTE) AS session_end
                        , CAST(COLLECT(url) AS STRING) AS url_list
                        FROM {source_table}
                        GROUP BY SESSION(window_timestamp, INTERVAL '5' MINUTE), ip, host
                    
                    """
        )



    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_aggregation()


