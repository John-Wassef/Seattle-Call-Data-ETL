from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, when, regexp_replace, trim
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, IntegerType
import time
import os

# Configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "seattle_police_calls"
CASSANDRA_HOST = "localhost"
CASSANDRA_PORT = "9042"
CASSANDRA_KEYSPACE = "seattle_data"
CASSANDRA_TABLE = "police_calls"

# Define schema based on API column names
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

def create_schema():
    return StructType([
        # String fields
        StructField("cad_event_number", StringType()),
        StructField("cad_event_clearance_description", StringType()),
        StructField("call_type", StringType()),
        StructField("priority", StringType()),
        StructField("initial_call_type", StringType()),
        StructField("final_call_type", StringType()),
        
        # Timestamp fields
        StructField("cad_event_original_time_queued", TimestampType()),
        StructField("cad_event_arrived_time", TimestampType()),
        
        # Location fields
        StructField("dispatch_precinct", StringType()),
        StructField("dispatch_sector", StringType()),
        StructField("dispatch_beat", StringType()),
        StructField("dispatch_longitude", StringType()),
        StructField("dispatch_latitude", StringType()),
        StructField("dispatch_reporting_area", StringType()),
        
        # Additional string fields
        StructField("cad_event_response_category", StringType()),
        StructField("call_sign_dispatch_id", StringType()),
        
        # More timestamp fields
        StructField("call_sign_dispatch_time", TimestampType()),
        StructField("first_care_call_sign_at_scene_time", TimestampType()),
        StructField("first_care_call_sign_dispatch_time", TimestampType()),
        StructField("first_co_response_call_sign_at_scene_time", TimestampType()),
        StructField("first_co_response_call_sign_dispatch_time", TimestampType()),
        StructField("first_spd_call_sign_at_scene_time", StringType()),
        StructField("first_spd_call_sign_dispatch_time", StringType()),
        StructField("last_care_call_sign_in_service_time", StringType()),
        StructField("last_co_response_call_sign_in_service_time", TimestampType()),
        StructField("last_spd_call_sign_in_service_time", TimestampType()),
        
        # Numeric fields converted to StringType
        StructField("care_call_sign_total_service_time_s_", StringType()),
        StructField("co_response_call_sign_total_service_time_s_", StringType()),
        StructField("spd_call_sign_total_service_time_s_", StringType()),
        StructField("call_sign_total_service_time_s_", StringType()),
        StructField("first_care_call_sign_dispatch_delay_time_s_", StringType()),
        StructField("first_care_call_sign_response_time_s_", StringType()),
        StructField("first_co_response_call_sign_dispatch_delay_time_s_", StringType()),
        StructField("first_co_response_call_sign_response_time_s_", StringType()),
        StructField("first_spd_call_sign_dispatch_delay_time_s_", StringType()),
        StructField("first_spd_call_sign_response_time_s_", StringType()),
        StructField("call_sign_dispatch_delay_time_s_", StringType()),
        StructField("call_sign_response_time_s_", StringType()),
        
        # Additional timestamps
        StructField("call_sign_at_scene_time", TimestampType()),
        StructField("cad_event_first_response_time_s_", StringType()),
        StructField("call_sign_in_service_time", TimestampType()),
        
        # Additional string fields
        StructField("call_type_indicator", StringType()),
        StructField("dispatch_neighborhood", StringType()),
        StructField("call_type_received_classification", StringType()),
        
        # Our added processing timestamp
        StructField("processed_at", StringType())
    ])

def create_cassandra_keyspace_and_table():
    """Initialize the Cassandra keyspace and table if they don't exist"""
    from cassandra.cluster import Cluster
    
    try:
        # Connect to Cassandra
        print("Connecting to Cassandra...")
        cluster = Cluster([CASSANDRA_HOST], port=int(CASSANDRA_PORT))
        session = cluster.connect()
        
        # Create keyspace if not exists
        print(f"Creating keyspace {CASSANDRA_KEYSPACE} if it doesn't exist...")
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)
        
        # Use the keyspace
        session.execute(f"USE {CASSANDRA_KEYSPACE}")
        
        # Create table if not exists
        print(f"Creating table {CASSANDRA_TABLE} if it doesn't exist...")
        session.execute(f"""
            CREATE TABLE IF NOT EXISTS {CASSANDRA_TABLE} (
                cad_event_number text PRIMARY KEY,
                cad_event_clearance_description text,
                call_type text,
                priority text,
                initial_call_type text,
                final_call_type text,
                cad_event_original_time_queued timestamp,
                cad_event_arrived_time timestamp,
                dispatch_precinct text,
                dispatch_sector text,
                dispatch_beat text,
                dispatch_longitude text,
                dispatch_latitude text,
                dispatch_reporting_area text,
                cad_event_response_category text,
                call_sign_dispatch_id text,
                call_sign_dispatch_time timestamp,
                first_care_call_sign_at_scene_time timestamp,
                first_care_call_sign_dispatch_time timestamp,
                first_co_response_call_sign_at_scene_time timestamp,
                first_co_response_call_sign_dispatch_time timestamp,
                first_spd_call_sign_at_scene_time text,
                first_spd_call_sign_dispatch_time text,
                last_care_call_sign_in_service_time text,
                last_co_response_call_sign_in_service_time timestamp,
                last_spd_call_sign_in_service_time timestamp,
                care_call_sign_total_service_time_s_ int,
                co_response_call_sign_total_service_time_s_ int,
                spd_call_sign_total_service_time_s_ int,
                call_sign_total_service_time_s_ int,
                first_care_call_sign_dispatch_delay_time_s_ int,
                first_care_call_sign_response_time_s_ int,
                first_co_response_call_sign_dispatch_delay_time_s_ int,
                first_co_response_call_sign_response_time_s_ int,
                first_spd_call_sign_dispatch_delay_time_s_ int,
                first_spd_call_sign_response_time_s_ int,
                call_sign_dispatch_delay_time_s_ int,
                call_sign_response_time_s_ int,
                call_sign_at_scene_time timestamp,
                cad_event_first_response_time_s_ int,
                call_sign_in_service_time timestamp,
                call_type_indicator text,
                dispatch_neighborhood text,
                call_type_received_classification text,
                processed_at text,
                insert_timestamp timestamp
            )
        """)
        
        print("Cassandra keyspace and table setup complete!")
        
    except Exception as e:
        print(f"Error setting up Cassandra: {e}")
    finally:
        if 'cluster' in locals():
            cluster.shutdown()

def transform_time_fields_to_int(df):
    """Transform string time fields to integers"""
    # List of columns to convert from string to integer
    time_columns = [
        "care_call_sign_total_service_time_s_",
        "co_response_call_sign_total_service_time_s_",
        "spd_call_sign_total_service_time_s_",
        "call_sign_total_service_time_s_",
        "first_care_call_sign_dispatch_delay_time_s_",
        "first_care_call_sign_response_time_s_",
        "first_co_response_call_sign_dispatch_delay_time_s_",
        "first_co_response_call_sign_response_time_s_",
        "first_spd_call_sign_dispatch_delay_time_s_",
        "first_spd_call_sign_response_time_s_",
        "call_sign_dispatch_delay_time_s_",
        "call_sign_response_time_s_",
        "cad_event_first_response_time_s_"
    ]
    
    # Apply transformations to each column
    for column in time_columns:
        df = df.withColumn(
            column,
            # Clean the string, remove non-numeric chars, and convert to integer
            when(
                col(column).isNotNull(),
                # Convert to integer, handling nulls and cleaning the string
                trim(regexp_replace(col(column), "[^0-9]", "")).cast(IntegerType())
            )
        )
    
    return df

def foreach_batch_function(df, epoch_id):
    """Process each micro-batch of data"""
    try:
        # Transform string time fields to integers
        transformed_df = transform_time_fields_to_int(df)
        
        # Add a timestamp for when the record was inserted into Cassandra
        df_with_timestamp = transformed_df.withColumn("insert_timestamp", current_timestamp())
        
        # Count records in the batch
        count = df_with_timestamp.count()
        print(f"Processing batch {epoch_id} with {count} records")
        
        if count > 0:
            # Show sample data (first few records)
            print("Sample data in this batch:")
            df_with_timestamp.show(5, truncate=False)
            
            # Write to Cassandra
            print(f"Writing batch {epoch_id} to Cassandra...")
            df_with_timestamp.write \
                .format("org.apache.spark.sql.cassandra") \
                .options(keyspace=CASSANDRA_KEYSPACE, table=CASSANDRA_TABLE) \
                .mode("append") \
                .save()
            
            print(f"Successfully wrote batch {epoch_id} to Cassandra")
    except Exception as e:
        print(f"Error processing batch {epoch_id}: {e}")

def start_streaming():
    """Start the Spark Streaming job"""
    # Create Spark Session
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("SeattlePoliceCallsConsumer") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                "com.github.jnr:jnr-posix:3.1.15") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"Connecting to Kafka broker at {KAFKA_BROKER}...")
    # Create streaming DataFrame from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()
    
    print("Connected to Kafka, setting up schema for data parsing...")
    # Parse value from Kafka
    schema = create_schema()
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Write the stream to Cassandra
    print("Starting streaming to Cassandra...")
    query = parsed_df.writeStream \
        .foreachBatch(foreach_batch_function) \
        .outputMode("update") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    print("Streaming is active. Waiting for data...")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping the streaming job...")
        query.stop()
        print("Streaming job stopped")

if __name__ == "__main__":
    # First set up Cassandra
    print("Setting up Cassandra keyspace and table...")
    create_cassandra_keyspace_and_table()
    
    # Wait a bit for Cassandra to fully initialize
    print("Waiting for Cassandra to fully initialize...")
    time.sleep(5)
    
    # Start the streaming job
    print("Starting the Spark Streaming job...")
    start_streaming()
