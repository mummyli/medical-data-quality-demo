#!/usr/bin/env python3
import argparse, json, time
from kafka import KafkaProducer

def produce_events(bootstrap, count, topic="clinical-events", interval_ms=0):
    producer = KafkaProducer(bootstrap_servers=bootstrap.split(","),
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    for i in range(count):
        event = {
            "event_id": f"evt-{i}",
            "patient_id": f"patient-{(i%50)+1}" if i % 20 != 0 else "",
            "visit_id": f"visit-{(i%100)+1}",
            "age": (i % 90) if i % 50 != 0 else 999,
            "admission_date": "2025-01-01",
            "discharge_date": "2025-01-02",
            "admission_type": "EMERGENCY" if i%7!=0 else "UNKNOWN",
            "ssn": f"ssn-{i}",
            "diagnosis": "A01",
            "lab_unit": "mmol/L" if i%10!=0 else "UNKNOWN_UNIT",
            "ts": None
        }
        producer.send(topic, event)
        if interval_ms:
            time.sleep(interval_ms/1000.0)
    producer.flush()
    print(f"Produced {count} events to {bootstrap}/{topic}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", type=str, default="localhost:9092")
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--topic", type=str, default="clinical-events")
    args = parser.parse_args()
    produce_events(args.bootstrap, args.count, args.topic)