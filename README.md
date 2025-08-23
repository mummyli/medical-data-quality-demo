# 📌 Medical Data Quality (MDQ)

## Overview

**Medical Data Quality (MDQ)** is a rule-based validation framework for medical data. 
It amis to ensures **completeness, consistency, and validity** of clinical event streams by applying configurable rules before the data enters downstream systems such as data warehouses or analytics pipelines.

Typical use cases include:

* Real-time monitoring of clinical event data
* Healthcare data governance
* Pre-ingestion validation before storing data in a data lake/warehouse
* Cross-system data reconciliation

---

## Features

* 🔎 **Flexible Rule Engine**: Supports required fields, length checks, numeric ranges, regex validation, cross-field comparison, domain checks, lookup validations, and time difference checks.
* ⚡ **Real-time Streaming**: Consumes messages from Kafka and validates events on the fly.
* 📂 **Pluggable Output Writers**: Validation results can be written to local files, S3, or external systems.
* 🔄 **Dynamic Lookup Tables**: Supports loading and refreshing lookup datasets from JSONL, local files, or S3.
* 🧩 **Extensible Design**: Add new rules by implementing `RuleStrategy` or new outputs by extending `OutputWriter`.

---

## Project Structure

```
medical-data-quality/
├── job/
|   ├── processor.py              # Main entrypoint: Kafka consumer and batch processor
├── core/
│   ├── rule_strategy.py      # Rule strategies implementation
│   ├── result_writer.py      # Output writer interface and implementations
│   └── lookup_loader.py      # Lookup table loader module
├── rules/
│   └── sample_rules.yaml     # Example rules configuration
├── references/
│   └── schedule.jsonl        # Example lookup reference table
├── requirements.txt          # Python dependencies
└── README.md                 # Project documentation
```

---

## Installation & Usage

### 1. Setup environment

```bash
# Create a virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Start Kafka

```bash
cd infra/
docker-compose up -d
```

### 3. Run the processor

```bash
python processor.py \
  --bootstrap localhost:9092 \
  --topic clinical-events \
  --rules ./rules/sample_rules.yaml \
  --output_type local \
  --output_dir ./result
```

---

## Rule Configuration (YAML)

Rules are defined in YAML files under `rules/`. Example:

```yaml
rules:
  - id: R001
    description: "admission_type must exist, but it can be empty"
    type: field_exist
    field: admission_type

  - id: R002
    description: "patient_id must exist and be non-empty"
    type: required
    field: patient_id

  - id: R003
    description: "ssn must greater than 5 and less than 11"
    type: value_length
    field: patient_id
    params:
        min: 5
        max: 10
```

---

## Example Input & Output

### Input (Kafka event)

```json
{
    "event_id": "evt-0",
    "patient_id": "patient-1",
    "visit_id": "visit-1",
    "age": 84,
    "admission_date": "2025-11-27",
    "admission_time": "20:34:31",
    "discharge_date": "2025-11-30",
    "admission_type": "EMERGENCY",
    "ssn": "7caceb9abb",
    "diagnosis": "C03",
    "lab_unit": "mmol/L",
    "ts": "2025-08-23T02:11:26.010315Z"
}
```

### Output (local file)

```json
{
    "event_id": "evt-0",
    "patient_id": "patient-1",
    "visit_id": "visit-1",
    "age": 84,
    "admission_date": "2025-11-27",
    "admission_time": "20:34:31",
    "discharge_date": "2025-11-30",
    "admission_type": "EMERGENCY",
    "ssn": "7caceb9abb",
    "diagnosis": "C03",
    "lab_unit": "mmol/L",
    "ts": "2025-08-23T02:11:26.010315Z",
    "_mdq_failures": [
        "R008: ts must be valid datetime",
        "CT101: admission_type must exist in domain_codes",
        "CT102: check column one by one",
        "CT103: event.ts should be within 48h of schedule.scheduled_time"
    ]
}
```

---

## Extending the Framework

* **New Rule**: Implement a subclass of `RuleStrategy` in `rule_strategy.py` and register it in `RuleStrategyFactory`.
* **New Output Writer**: Extend `OutputWriter` in `result_writer.py`.
* **New Lookup Source**: Add loader logic in `lookup_loader.py`.

