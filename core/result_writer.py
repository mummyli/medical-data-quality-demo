import json
import os
import time
from datetime import datetime, timezone
from abc import ABC, abstractmethod

import boto3

class OutputWriter(ABC):
    @abstractmethod
    def write_batch(self, batch, batch_index):
        pass


class S3OutputWriter(OutputWriter):
    def __init__(self, bucket):
        self.bucket = bucket
        self.s3 = boto3.client("s3")
        self.out_key_prefix = "processed/" + datetime.now(timezone.utc).strftime("%Y-%m-%d/")

    def write_batch(self, batch, batch_index):
        key = self.out_key_prefix + f"batch-{batch_index}.jsonl"
        payload = "\n".join([json.dumps(e, ensure_ascii=False) for e in batch]).encode("utf-8")
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=payload)
        return f"s3://{self.bucket}/{key}"

class LocalFileOutputWriter(OutputWriter):
    def __init__(self, output_dir):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def write_batch(self, batch, batch_index):
        filename = f"batch-{batch_index}-{int(time.time())}.jsonl"
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            for event in batch:
                f.write(json.dumps(event, ensure_ascii=False) + '\n')
        return filepath


class DatabaseOutputWriter(OutputWriter):
    def __init__(self, connection_string):
        self.conn = None
        self.connection_string = connection_string

    def write_batch(self, batch, batch_index):
        print(f"Would write {len(batch)} events to database")
        return f"database://{self.connection_string}"

class OutputWriterFactory:
    @staticmethod
    def create_writer(output_type, **kwargs):
        if output_type == "s3":
            return S3OutputWriter(
                bucket=kwargs.get("bucket")
            )
        elif output_type == "local":
            return LocalFileOutputWriter(
                output_dir=kwargs.get("output_dir", "./output")
            )
        elif output_type == "database":
            return DatabaseOutputWriter(
                connection_string=kwargs.get("connection_string")
            )
        else:
            raise ValueError(f"Unsupported output type: {output_type}")
