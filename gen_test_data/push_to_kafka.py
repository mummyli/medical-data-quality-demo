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
        event, _ = ge.generate_event(i)
        producer.send(topic, event)
        
        if interval_ms:
            time.sleep(interval_ms/1000.0)
    producer.flush()
    print(f"Produced {count} events to {bootstrap}/{topic}")
    
def produce_events_save_local(localpath, bootstrap, count, topic="clinical-events", interval_ms=0):
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
    patient_admission_events = []
    for i in range(count):
        event, admission_event = ge.generate_event(i)
        producer.send(topic, event)
        
        patient_admission_events.append(admission_event)
        
        if interval_ms:
            time.sleep(interval_ms/1000.0)
    producer.flush()
    print(f"Produced {count} events to {bootstrap}/{topic}")
    
    outfile = f"{localpath}/patient_admission_event.jsonl"
    with open(outfile, "w", encoding="utf8") as f:
        for e in patient_admission_events:
            f.write(json.dumps(e))
            f.write("\n")
    print(f"Wrote {len(patient_admission_events)} events to {outfile}")
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", type=str, default="192.168.1.3:9092")
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--topic", type=str, default="clinical-events")
    parser.add_argument("--localpath", type=str, default="./references")
    args = parser.parse_args()
    
    produce_events_save_local(args.localpath, args.bootstrap, args.count, args.topic)