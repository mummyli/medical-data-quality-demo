import argparse, json, random, datetime, hashlib


def generate_event(i):
    '''
    generate patient event for test.
    '''
    pid = f"patient-{(i%50)+1}"
    visit_id = f"visit-{(i%100)+1}"
    admit = datetime.date(2025,1,1) + datetime.timedelta(days=random.randint(0,365))
    discharge = admit + datetime.timedelta(days=random.randint(0,10))
    if random.random() < 0.05:
        age = 999  # generate incorrect value
    else:
        age = random.randint(0,90)
    admission_type = random.choice(["EMERGENCY", "INPATIENT", "OUTPATIENT", "UNKNOWN"])
    lab_unit = random.choice(["mmol/L","mg/dL","UNKNOWN_UNIT"])
    event = {
        "event_id": f"evt-{i}",
        "patient_id": pid,
        "visit_id": visit_id,
        "age": age,
        "admission_date": admit.isoformat(),
        "discharge_date": discharge.isoformat(),
        "admission_type": admission_type,
        "ssn": hashlib.sha1(pid.encode()).hexdigest()[:10] if pid else "",
        "diagnosis": random.choice(["A01","B02","C03","D04"]),
        "lab_unit": lab_unit,
        "ts": datetime.datetime.utcnow().isoformat() + "Z"
    }
    return event


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--outfile", type=str, default=None)
    args = parser.parse_args()
    
    events = [generate_event(i) for i in range(args.count)]
    
    if args.outfile:
        with open(args.outfile, "w", encoding="utf8") as f:
            for e in events:
                f.write(json.dumps(e))
                f.write("\n")
        print(f"Wrote {len(events)} events to {args.outfile}")
    else:
        for e in events:
            print(json.dumps(e))

if __name__ == "__main__":
    main()