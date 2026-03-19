from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def create_events_aggregated_sink(t_env):
    table_name = 'green_trips_aggregated'
    sink_ddl = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            window_start TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT,
            total_revenue DOUBLE,
            PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_events_source_kafka(t_env):
    table_name = "green_trips"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INT,
            DOLocationID INT,
            trip_distance DOUBLE,
            total_amount DOUBLE,
            lpep_pickup_datetime STRING,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:9092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        )
    """
    t_env.execute_sql(source_ddl)
    return table_name

def log_aggregation():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)  # checkpoint every 10 seconds
    env.set_parallelism(1)  # single-partition Kafka topic

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka source table
        source_table = create_events_source_kafka(t_env)
        # Create PostgreSQL sink table
        aggregated_table = create_events_aggregated_sink(t_env)

        # 5-minute tumbling window aggregation
        t_env.execute_sql(f"""
            INSERT INTO {aggregated_table}
            SELECT
                TUMBLE_START(event_timestamp, INTERVAL '5' MINUTE) AS window_start,
                PULocationID,
                COUNT(*) AS num_trips,
                SUM(total_amount) AS total_revenue
            FROM {source_table}
            GROUP BY
                PULocationID,
                TUMBLE(event_timestamp, INTERVAL '5' MINUTE)
        """).wait()

    except Exception as e:
        print("Writing records from Kafka to PostgreSQL failed:", str(e))

if __name__ == '__main__':
    log_aggregation()