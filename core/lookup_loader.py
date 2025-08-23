# - Supports file://, s3://, local path
# - Supports .json (array) and .jsonl (one JSON per line)
# - Keeps small tables in memory, marks large ones as too_large for on-demand scanning
import os
import json
import threading
import time
from urllib.parse import urlparse

try:
    import boto3
except Exception:
    boto3 = None

_LOOKUPS = {}
_LOOKUPS_META = {}
_LOCK = threading.RLock()

DEFAULT_MAX_IN_MEMORY_BYTES = 50 * 1024 * 1024


def _read_jsonl_lines_from_file(path):
    with open(path, 'r', encoding='utf8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _read_json_array_from_file(path):
    with open(path, 'r', encoding='utf8') as f:
        return json.load(f)


def _read_jsonl_from_s3(bucket, key, s3_client):
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    body = obj['Body']
    
    for raw_line in body.iter_lines(decode_unicode=True):
        line = raw_line.strip()
        if not line:
            continue
        yield json.loads(line)


def _list_s3_objects(bucket, prefix, s3_client):
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            yield obj['Key']


def _load_from_local_path(path):
    if os.path.isdir(path):
        def gen_dir():
            for fname in os.listdir(path):
                fpath = os.path.join(path, fname)
                if not os.path.isfile(fpath):
                    continue
                if fname.lower().endswith('.json'):
                    for rec in _read_json_array_from_file(fpath):
                        yield rec
                else:
                    for rec in _read_jsonl_lines_from_file(fpath):
                        yield rec
        return gen_dir()
    else:
        if path.lower().endswith('.json'):
            data = _read_json_array_from_file(path)
            return iter(data)
        else:
            return _read_jsonl_lines_from_file(path)


def _load_from_s3_uri(uri):
    if boto3 is None:
        raise RuntimeError("boto3 not available but s3 URI requested")
    s3_client = boto3.client('s3')
    parsed = urlparse(uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')
    if key == '' or key.endswith('/'):
        
        def gen_prefix():
            for obj_key in _list_s3_objects(bucket, key, s3_client):
                if obj_key.lower().endswith('.json'):
                    obj = s3_client.get_object(Bucket=bucket, Key=obj_key)
                    arr = json.loads(obj['Body'].read().decode('utf8'))
                    for rec in arr:
                        yield rec
                else:
                    for rec in _read_jsonl_from_s3(bucket, obj_key, s3_client):
                        yield rec
        return gen_prefix()
    else:
        if key.lower().endswith('.json'):
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            arr = json.loads(obj['Body'].read().decode('utf8'))
            return iter(arr)
        else:
            return _read_jsonl_from_s3(bucket, key, s3_client)


def load_lookup_loader(loader_uri: str, key_field: str = None, max_in_memory_bytes: int = DEFAULT_MAX_IN_MEMORY_BYTES):
    """
    Load a lookup resource from loader_uri and return a dict:
      { 'too_large': bool, 'size_bytes': int, 'count': int, 'table': dict_or_None,
        'loader_uri': loader_uri, 'key_field': key_field }
    If 'too_large' is True, 'table' will be None and caller may use loader_uri for on-demand scanning.
    """
    
    if loader_uri.startswith("file://"):
        path = loader_uri[len("file://"):]
        records_iter = _load_from_local_path(path)
    elif loader_uri.startswith("s3://"):
        records_iter = _load_from_s3_uri(loader_uri)
    else:
        records_iter = _load_from_local_path(loader_uri)

    temp = []
    total_size = 0
    count = 0
    for rec in records_iter:
        count += 1
        total_size += len(json.dumps(rec, ensure_ascii=False))
        temp.append(rec)
        if total_size > max_in_memory_bytes:
            
            return {
                'too_large': True,
                'size_bytes': total_size,
                'count': count,
                'table': None,
                'loader_uri': loader_uri,
                'key_field': key_field
            }

    
    keyed = {}
    for rec in temp:
        if key_field:
            k = rec.get(key_field)
        else:
            k = rec.get('patient_id') or rec.get('visit_id') or rec.get('id') or None
        if k is not None:
            keyed[k] = rec

    return {
        'too_large': False,
        'size_bytes': total_size,
        'count': count,
        'table': keyed,
        'loader_uri': loader_uri,
        'key_field': key_field
    }


def load_lookups(rules: list, max_in_memory_bytes: int = DEFAULT_MAX_IN_MEMORY_BYTES, logger=None):
    """
    Load all lookups described in rules into in-memory map (atomic replace).
    """
    new_lookups = {}
    new_meta = {}
    for r in rules:
        if 'lookup' not in r:
            continue
        lookup = r['lookup']
        loader = lookup.get('loader')
        name = lookup.get('name')
        key_field = lookup.get('key') or lookup.get('key_field') or None
        if not loader or not name:
            if logger:
                logger.warning("lookup missing loader or name: %s", r)
            continue
        try:
            res = load_lookup_loader(loader, key_field=key_field, max_in_memory_bytes=max_in_memory_bytes)
            if res['too_large']:
                new_lookups[name] = None
                new_meta[name] = {
                    'too_large': True,
                    'size_bytes': res['size_bytes'],
                    'count': res['count'],
                    'loader_uri': res['loader_uri'],
                    'key_field': res['key_field'],
                    'last_loaded': time.time()
                }
                if logger:
                    logger.info("[lookup] %s marked too_large size=%s count=%s", name, res['size_bytes'], res['count'])
            else:
                new_lookups[name] = res['table']
                new_meta[name] = {
                    'too_large': False,
                    'size_bytes': res['size_bytes'],
                    'count': res['count'],
                    'loader_uri': res['loader_uri'],
                    'key_field': res['key_field'],
                    'last_loaded': time.time()
                }
                if logger:
                    logger.info("[lookup] loaded %s count=%s size=%s", name, res['count'], res['size_bytes'])
        except Exception as e:
            new_lookups[name] = None
            new_meta[name] = {
                'too_large': False,
                'error': str(e),
                'last_loaded': time.time()
            }
            if logger:
                logger.exception("[lookup] failed to load %s: %s", name, e)

    
    with _LOCK:
        _LOOKUPS.clear()
        _LOOKUPS.update(new_lookups)
        _LOOKUPS_META.clear()
        _LOOKUPS_META.update(new_meta)


def reload_lookups_periodically(rules, interval=300, stop_event=None, logger=None, max_in_memory_bytes: int = DEFAULT_MAX_IN_MEMORY_BYTES):
    """Background thread target: reload lookups periodically."""
    while True:
        try:
            load_lookups(rules, max_in_memory_bytes=max_in_memory_bytes, logger=logger)
        except Exception as e:
            if logger:
                logger.exception("error reloading lookups: %s", e)
        if stop_event and stop_event.is_set():
            break
        time.sleep(interval)


def get_lookup_record(name: str, key):
    """
    Thread-safe accessor. If table is in memory, return table[key].
    If table was marked too_large, perform on-demand scan (inefficient) using loader_uri from meta.
    Returns record dict or None.
    """
    with _LOCK:
        table = _LOOKUPS.get(name)
        meta = _LOOKUPS_META.get(name, {})

    if table is None:
        
        loader_uri = meta.get('loader_uri')
        key_field = meta.get('key_field')
        if not loader_uri:
            return None
        
        if loader_uri.startswith("file://"):
            path = loader_uri[len("file://"):]
            for rec in _load_from_local_path(path):
                k = rec.get(key_field) if key_field else (rec.get('patient_id') or rec.get('visit_id') or rec.get('id'))
                if k == key:
                    return rec
            return None
        elif loader_uri.startswith("s3://"):
            for rec in _load_from_s3_uri(loader_uri):
                k = rec.get(key_field) if key_field else (rec.get('patient_id') or rec.get('visit_id') or rec.get('id'))
                if k == key:
                    return rec
            return None
        else:
            for rec in _load_from_local_path(loader_uri):
                k = rec.get(key_field) if key_field else (rec.get('patient_id') or rec.get('visit_id') or rec.get('id'))
                if k == key:
                    return rec
            return None
    else:
        return table.get(key)


def get_lookup_table(name: str):
    """Return the in-memory table or None (if too large/unavailable)."""
    with _LOCK:
        return _LOOKUPS.get(name)
