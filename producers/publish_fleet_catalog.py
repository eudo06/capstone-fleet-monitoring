"""
Publish the fleet catalog (40 vehicles) to the compacted topic `fleet_catalog`.

- key = vehicle_id, so compaction keeps the latest record per vehicle;
- idempotent producer (enable.idempotence=True implies acks=all and retries without
  duplicates or reordering inside a partition);
- deterministic content (fixed seed): running the script twice publishes exactly the
  same records, so a re-run only adds new versions that compaction will later collapse.

Values follow the README of the subject:
  regions : Paris, Lyon, Marseille, Lille, Toulouse
  types   : van, truck, refrigerated_truck, motorbike
  payload : 500, 1000, 1500 or 3000 kg

Usage: python3 producers/publish_fleet_catalog.py
"""
import json
import os
import random
import string
import sys

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "fleet_catalog"
NUM_VEHICLES = 40
SEED = 42

REGIONS = ["Paris", "Lyon", "Marseille", "Lille", "Toulouse"]

# Each vehicle type gets a payload consistent with what it can carry.
PAYLOAD_BY_TYPE = {
    "motorbike": [500],
    "van": [1000, 1500],
    "refrigerated_truck": [1500, 3000],
    "truck": [3000],
}
# Mostly vans and trucks, a few motorbikes: a plausible delivery fleet.
TYPE_WEIGHTS = {"van": 0.45, "truck": 0.25, "refrigerated_truck": 0.20, "motorbike": 0.10}

# French SIV plates (AA-123-AA) never use the letters I, O and U.
PLATE_LETTERS = [c for c in string.ascii_uppercase if c not in "IOU"]


def make_plate(rng: random.Random, used: set) -> str:
    while True:
        plate = (
            f"{rng.choice(PLATE_LETTERS)}{rng.choice(PLATE_LETTERS)}-"
            f"{rng.randint(1, 999):03d}-"
            f"{rng.choice(PLATE_LETTERS)}{rng.choice(PLATE_LETTERS)}"
        )
        if plate not in used:
            used.add(plate)
            return plate


def build_catalog() -> list[dict]:
    rng = random.Random(SEED)
    used_plates: set = set()
    types = list(TYPE_WEIGHTS)
    weights = list(TYPE_WEIGHTS.values())

    catalog = []
    for i in range(1, NUM_VEHICLES + 1):
        vehicle_type = rng.choices(types, weights=weights, k=1)[0]
        catalog.append({
            "vehicle_id": f"V{i:04d}",
            "plate": make_plate(rng, used_plates),
            "vehicle_type": vehicle_type,
            # Round-robin: exactly 8 vehicles per region, so regional KPIs are comparable.
            "home_region": REGIONS[(i - 1) % len(REGIONS)],
            "max_payload_kg": rng.choice(PAYLOAD_BY_TYPE[vehicle_type]),
        })
    return catalog


def main() -> None:
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "enable.idempotence": True,
        "acks": "all",
    })

    failures = []

    def on_delivery(err, msg):
        if err is not None:
            failures.append((msg.key(), err))
        else:
            print(f"{msg.key().decode()} -> {msg.topic()}[{msg.partition()}] @ {msg.offset()}")

    catalog = build_catalog()
    for vehicle in catalog:
        producer.produce(
            topic=TOPIC,
            key=vehicle["vehicle_id"].encode("utf-8"),
            value=json.dumps(vehicle).encode("utf-8"),
            on_delivery=on_delivery,
        )
        producer.poll(0)  # serve delivery callbacks as we go

    remaining = producer.flush(30)

    if remaining or failures:
        for key, err in failures:
            print(f"delivery failed for {key}: {err}", file=sys.stderr)
        print(f"{remaining} message(s) still in queue, {len(failures)} failed", file=sys.stderr)
        sys.exit(1)

    print(f"published {len(catalog)} vehicles to {TOPIC}")


if __name__ == "__main__":
    main()