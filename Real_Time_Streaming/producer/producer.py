import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime

# Configuration
API_URL = "https://data.seattle.gov/resource/33kz-ixgy.json"
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "seattle_police_calls"
POLL_INTERVAL = 20  # seconds

def fetch_data():
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(2, 0, 2)
    )

def publish_data(producer, data):
    for record in data:
        try:
            # Add processing timestamp
            record['processed_at'] = datetime.utcnow().isoformat()
            producer.send(TOPIC_NAME, value=record)
            print(f"Sent record: {record.get('cad_event_number', 'No ID')}")
        except Exception as e:
            print(f"Error sending record: {e}")

def run_producer():
    producer = create_producer()
    print("Producer started. Press Ctrl+C to stop.")
    
    try:
        while True:
            data = fetch_data()
            if data:
                publish_data(producer, data)
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    run_producer()