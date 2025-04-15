from cassandra.cluster import Cluster
import sys

# Configuration
CASSANDRA_HOST = "localhost"
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = "seattle_data"
CASSANDRA_TABLE = "police_calls"

def query_cassandra(limit=10):
    """Query the Cassandra table and display results"""
    try:
        # Connect to Cassandra
        print(f"Connecting to Cassandra at {CASSANDRA_HOST}:{CASSANDRA_PORT}...")
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect(CASSANDRA_KEYSPACE)
        
        # Query the table
        print(f"Querying table {CASSANDRA_TABLE}...")
        rows = session.execute(f"SELECT * FROM {CASSANDRA_TABLE} LIMIT {limit}")
        
        # Display results
        row_count = 0
        for row in rows:
            row_count += 1
            print(f"\n--- Record {row_count} ---")
            print(f"CAD Event Number: {row.cad_event_number}")
            print(f"Call Type: {row.call_type}")
            print(f"Priority: {row.priority}")
            print(f"Dispatch Precinct: {row.dispatch_precinct}")
            print(f"Processed At: {row.processed_at}")
            print(f"Insert Timestamp: {row.insert_timestamp}")
        
        # Get total count
        count_row = session.execute(f"SELECT COUNT(*) FROM {CASSANDRA_TABLE}").one()
        total_count = count_row[0] if count_row else 0
        
        print(f"\nShowing {row_count} of {total_count} total records in {CASSANDRA_TABLE}")
        
    except Exception as e:
        print(f"Error querying Cassandra: {e}")
    finally:
        if 'cluster' in locals():
            cluster.shutdown()

if __name__ == "__main__":
    # Get limit from command line if provided
    limit = 10
    if len(sys.argv) > 1:
        try:
            limit = int(sys.argv[1])
        except ValueError:
            print(f"Invalid limit: {sys.argv[1]}. Using default: 10")
    
    query_cassandra(limit)
