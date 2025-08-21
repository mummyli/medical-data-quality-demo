import argparse
import os
import yaml
from kafka import KafkaConsumer
import json
from urllib.parse import urlparse
import boto3
import threading
import time
from datetime import datetime, timezone

LOOKUPS={}

def load_lookup_loader(loader_uri):
    if loader_uri.startswith("file://"):
        path = loader_uri[len("file://"):]
        table = {}
        with open(path, "r", encoding='utf8') as f:
            for line in f:
                if not line.strip(): continue
                rec = json.loads(line)
                key = rec.get('patient_id') or rec.get('visit_id') or rec.get('id') or None
                if key:
                    table[key] = rec
        return table
    elif loader_uri.startswith("s3://"):
        parsed = urlparse(loader_uri)
        bucket = parsed.netloc
        key = parsed.path.lstrip('/')
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read().decode('utf8').splitlines()
        table = {}
        for line in body:
            if not line.strip(): continue
            rec = json.loads(line)
            key = rec.get('patient_id') or rec.get('visit_id') or rec.get('id') or None
            if key:
                table[key] = rec
        return table
    elif loader_uri.endswith('.json'):
        with open(loader_uri, 'r', encoding='utf8') as f:
            return json.load(f)
    else:
        if os.path.exists(loader_uri):
            table = {}
            with open(loader_uri, 'r', encoding='utf8') as f:
                for line in f:
                    if not line.strip(): continue
                    rec = json.loads(line)
                    key = rec.get('patient_id') or rec.get('visit_id') or rec.get('id') or None
                    if key:
                        table[key] = rec
            return table
    return {}


def load_lookups(rules):
    for r in rules:
        if 'lookup' in r:
            loader = r['lookup'].get('loader')
            name = r['lookup'].get('name')
            if loader and name:
                try:
                    LOOKUPS[name] = load_lookup_loader(loader)
                    print(f"[lookup] loaded {name} ({len(LOOKUPS[name])} records) from {loader}")
                except Exception as e:
                    print(f"[lookup] failed to load {name} {loader} {e}")

def reload_lookups_periodically(rules, interval=300):
    while True:
        load_lookups(rules)
        time.sleep(interval)
        
        
def apply_single_rules(event, rules):
    pass


def apply_cross_table_rules(event, rules):
    pass


def apply_reconciliation_rules(event, rules):
    pass
        

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="clinical-events")
    parser.add_argument("--rules", default="./rules/sample_rules.yaml")
    parser.add_argument("--bucket", default="clinical-bucket")
    args = parser.parse_args()

    with open(os.path.abspath(args.rules),'r',encoding='utf8') as f:
        rules = yaml.safe_load(f)['rules']
        print(rules)

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

    s3 = boto3.client("s3",
                      aws_access_key_id="access_key",
                      aws_secret_access_key="secret_access_key")

    
    out_key_prefix = "processed/" + datetime.now(timezone.utc).strftime("%Y-%m-%d/")

    print("Processor started. Listening for events...")
    batch = []
    count = 0
    for msg in consumer:
        event = msg.value
        failures = []
        failures.extend(apply_single_rules(event, rules))
        failures.extend(apply_cross_table_rules(event, rules))
        failures.extend(apply_reconciliation_rules(event, rules))
        event["_mdq_failures"] = failures
        batch.append(event)
        count += 1
        if len(batch) >= 50:
            key = out_key_prefix + f"batch-{int(time.time())}.jsonl"
            payload = "\n".join([json.dumps(e, ensure_ascii=False) for e in batch]).encode("utf-8")
            s3.put_object(Bucket=args.bucket, Key=key, Body=payload)
            print(f"Wrote {len(batch)} events to s3://{args.bucket}/{key}, failures in this batch: {sum(1 for e in batch if e['_mdq_failures'])}")
            batch = []
        

if __name__ == '__main__':
    main()