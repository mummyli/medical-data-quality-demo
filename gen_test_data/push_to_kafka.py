#!/usr/bin/env python3
import argparse, json, time
from kafka import KafkaProducer
import generate_events as ge

def produce_events(bootstrap, count, topic="clinical-events", interval_ms=0):
    '''
    generate event and push to kafka
    '''
    producer = KafkaProducer(bootstrap_servers=bootstrap.split(","),
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    
    for i in range(count):
        
        event = ge.generate_event(i)
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