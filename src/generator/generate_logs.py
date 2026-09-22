import time
import json
import uuid
import random
from datetime import datetime
from confluent_kafka import Producer

KAFKA_BROKER = 'kafka:29092' # Uses the docker network internal listener
TOPIC = 'telemetry_logs'

# Configuration for Producer
conf = {
    'bootstrap.servers': KAFKA_BROKER,
}

producer = Producer(conf)

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

endpoints = ['/home', '/login', '/api/data', '/checkout', '/profile']
status_codes = [200, 200, 200, 201, 400, 401, 404, 500]

def generate_log(record_count):
    log_id = str(uuid.uuid4())
    user_id = f"user_{random.randint(1, 100)}"
    timestamp = datetime.utcnow().isoformat() + "Z"
    endpoint = random.choice(endpoints)
    status_code = random.choice(status_codes)
    response_time_ms = random.randint(10, 2000)

    # Base payload
    payload = {
        "log_id": log_id,
        "user_id": user_id,
        "timestamp": timestamp,
        "endpoint": endpoint,
        "status_code": status_code,
        "response_time_ms": response_time_ms
    }

    # Data Quality Injection: Corrupt every 20th record
    if record_count % 20 == 0:
        corruption_type = random.choice(["null_user", "invalid_status", "missing_timestamp"])
        if corruption_type == "null_user":
            payload["user_id"] = None
        elif corruption_type == "invalid_status":
            payload["status_code"] = 999
        elif corruption_type == "missing_timestamp":
            del payload["timestamp"]
        print(f"--- Injected corruption: {corruption_type} ---")

    # Schema Evolution Trigger: After 100 records add new field
    if record_count > 100:
        payload["user_tier"] = random.choice(["premium", "free"])

    return payload

def main():
    record_count = 1
    print("Starting Data Generator...")
    while True:
        payload = generate_log(record_count)
        
        # Produce message to Kafka
        # We use the user_id as the key for partitioning if it exists
        key = payload.get("user_id")
        key_str = str(key) if key is not None else "unknown"

        producer.produce(
            TOPIC, 
            key=key_str, 
            value=json.dumps(payload),
            callback=delivery_report
        )
        
        # Trigger delivery reports for previous messages
        producer.poll(0)
        
        print(f"Produced record {record_count}: {json.dumps(payload)}")
        
        record_count += 1
        time.sleep(random.uniform(0.5, 2.0))

if __name__ == '__main__':
    # Initial sleep to give Kafka time to start up before connecting
    print("Waiting for Kafka to become available...")
    time.sleep(20)
    main()
