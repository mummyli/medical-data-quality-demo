#!/usr/bin/env python3
import argparse, json, time
from kafka import KafkaProducer
import generate_events as ge

def produce_events(bootstrap, count, topic="clinical-events", interval_ms=0):
    producer = KafkaProducer(
            bootstrap_servers=bootstrap.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=60000,
            retry_backoff_ms=1000,
            api_version=(3, 8, 0),
            security_protocol='PLAINTEXT',
            connections_max_idle_ms=10000,
            reconnect_backoff_ms=1000,
            reconnect_backoff_max_ms=10000,
            metadata_max_age_ms=300000,
            api_version_auto_timeout_ms=30000
        )
    
    for i in range(count):
        event = ge.generate_event(i)
        producer.send(topic, event)
        
        if interval_ms:
            time.sleep(interval_ms/1000.0)
    producer.flush()
    print(f"Produced {count} events to {bootstrap}/{topic}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", type=str, default="192.168.1.3:9092")
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--topic", type=str, default="clinical-events")
    args = parser.parse_args()
    
    produce_events(args.bootstrap, args.count, args.topic)