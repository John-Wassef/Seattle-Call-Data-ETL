from cassandra.cluster import Cluster
import time

# Configuration
CASSANDRA_HOST = "localhost"
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = "seattle_data"
CASSANDRA_TABLE = "police_calls"

def setup_cassandra():
    """Set up Cassandra keyspace and table"""
    max_retries = 5
    retry_interval = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            print(f"Connecting to Cassandra (attempt {attempt+1}/{max_retries})...")
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            session = cluster.connect()
            
            # Create keyspace
            print(f"Creating keyspace {CASSANDRA_KEYSPACE}...")
            session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """)
            
            # Use the keyspace
            session.execute(f"USE {CASSANDRA_KEYSPACE}")
            
            # Create table
            print(f"Creating table {CASSANDRA_TABLE}...")
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
            
            print("Successfully set up Cassandra keyspace and table!")
            cluster.shutdown()
            return True
            
        except Exception as e:
            print(f"Error setting up Cassandra (attempt {attempt+1}): {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                print("Max retries reached. Could not connect to Cassandra.")
                return False

if __name__ == "__main__":
    setup_cassandra()
