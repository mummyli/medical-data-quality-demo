import argparse
import os
import yaml
from kafka import KafkaConsumer
import json
import threading
import time
import core.rule_strategy as rs
from core.result_writer import OutputWriterFactory
from core.lookup_loader import load_lookups, reload_lookups_periodically
        

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="clinical-events")
    parser.add_argument("--rules", default="./rules/sample_rules.yaml")
    parser.add_argument("--bucket", default="clinical-bucket")
    parser.add_argument("--flush_interval", default="60")
    parser.add_argument("--batch_size", default="50")
    parser.add_argument("--output_type", default="local")
    parser.add_argument("--output_dir", default="./result")
    parser.add_argument("--connection_string", default="")
    args = parser.parse_args()

    with open(os.path.abspath(args.rules),'r',encoding='utf8') as f:
        rules = yaml.safe_load(f)['rules']

    load_lookups(rules)

    t = threading.Thread(target=reload_lookups_periodically, args=(rules,60), daemon=True)
    t.start()

    consumer = KafkaConsumer(args.topic,
                             bootstrap_servers=args.bootstrap.split(","),
                             value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                             auto_offset_reset="earliest",
                             enable_auto_commit=True,
                             group_id="mdq-group"
                             )

    output_writer = OutputWriterFactory.create_writer(
        args.output_type,
        bucket=args.bucket,
        output_dir=args.output_dir,
        connection_string=args.connection_string
    )

    print("Processor started. Listening for events...")
    batch = []
    count = 0
    batch_index = 0
    last_flush_time = time.time()
    
    try:
        for msg in consumer:
            event = msg.value
            failures = []
            failures.extend(rs.apply_rules(event, rules))
            event["_mdq_failures"] = failures
            batch.append(event)
            count += 1
            
            current_time = time.time()
            time_since_last_flush = current_time - last_flush_time
            
            if len(batch) >= int(args.batch_size) or time_since_last_flush >= int(args.flush_interval):
                if batch:
                    output_path = output_writer.write_batch(batch, batch_index)
                    failures_count = sum(1 for e in batch if e['_mdq_failures'])
                    print(f"Wrote {len(batch)} events to {output_path}, failures: {failures_count}")
                    batch = []
                    batch_index += 1
                    last_flush_time = current_time
                    
    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        if batch:
            output_path = output_writer.write_batch(batch, batch_index)
            failures_count = sum(1 for e in batch if e['_mdq_failures'])
            print(f"Wrote remaining {len(batch)} events to {output_path}, failures: {failures_count}")
        

if __name__ == '__main__':
    main()