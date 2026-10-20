"""
Capstone Project -- telemetry simulator with INJECTED FAULTS.

Besides normal events, the simulator deliberately produces the problems a
real pipeline must survive:
  * ~3%  anomalies         speed above 120 km/h OR harsh braking (point alerts)
  * sustained speeding     from time to time one vehicle drives at 101-118 km/h
                           for 6-9 minutes (sustained_speeding alert, Query 3)
  * ~1%  duplicates        the exact same message (same event_id) sent twice
  * ~2%  late events       event_time 3-6 minutes in the past
  * ~1%  corrupted events  broken JSON, missing field, impossible speed, bad timestamp
  * optional hot vehicle   --hot-vehicle sends ~40% of all traffic from V0001
                           (key skew: one partition gets overloaded)
  * optional unknown ids   --unknown-vehicles sends ~1% of traffic from V0041-V0045,
                           vehicles that are not in the catalog (region UNKNOWN)

Usage:  python3 producers/produce_vehicle_events.py [--rate 20] [--hot-vehicle] [--unknown-vehicles]
"""
import argparse
import json
import random
import signal
import time
import uuid
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "vehicle_events"
NUM_VEHICLES = 40
HOT_VEHICLE = "V0001"
UNKNOWN_VEHICLES = [f"V{i:04d}" for i in range(NUM_VEHICLES + 1, NUM_VEHICLES + 6)]

ANOMALY_RATE = 0.03
DUPLICATE_RATE = 0.01
LATE_RATE = 0.02
MALFORMED_RATE = 0.01
UNKNOWN_RATE = 0.01
HOT_SHARE = 0.4

# Sustained speeding: on average one new episode every SUSTAINED_EVERY_S seconds,
# at most SUSTAINED_MAX at the same time. An episode lasts longer than the 5-minute
# window of Query 3, so at least one full window sees only fast events.
SUSTAINED_EVERY_S = 240
SUSTAINED_MAX = 2
SUSTAINED_DURATION_S = (360, 540)

# One id per simulator run: event_ids stay unique when the simulator is restarted.
RUN_ID = uuid.uuid4().hex[:8]


def make_event(vehicle_id: str, seq: int, sustained: bool) -> dict:
    event_time = datetime.now(timezone.utc)
    if random.random() < LATE_RATE:
        event_time -= timedelta(minutes=random.uniform(3, 6))
    speed = random.uniform(101, 118) if sustained else random.uniform(10, 95)
    harsh_braking = False
    if random.random() < ANOMALY_RATE:
        if random.random() < 0.5:
            speed = random.uniform(121, 160)
        else:
            harsh_braking = True
    return {
        "event_id": f"{vehicle_id}-{RUN_ID}-{seq}",
        "vehicle_id": vehicle_id,
        # Always millisecond precision and an explicit UTC offset, e.g.
        # 2026-10-13T09:16:00.123+00:00 (Spark pattern yyyy-MM-dd'T'HH:mm:ss.SSSXXX).
        "event_time": event_time.isoformat(timespec="milliseconds"),
        "latitude": round(random.uniform(42.0, 51.0), 5),
        "longitude": round(random.uniform(-4.0, 8.0), 5),
        "speed_kmh": round(speed, 1),
        "fuel_level_pct": round(random.uniform(5, 100), 1),
        "harsh_braking": harsh_braking,
    }


def corrupt(event: dict) -> str:
    kind = random.choice(["broken_json", "missing_field", "bad_speed", "bad_time"])
    if kind == "broken_json":
        return json.dumps(event)[:-7]
    if kind == "missing_field":
        event = dict(event)
        event.pop(random.choice(["vehicle_id", "event_time", "speed_kmh"]))
    elif kind == "bad_speed":
        event = dict(event, speed_kmh=random.choice([-12.0, 999.0]))
    else:
        event = dict(event, event_time="not-a-timestamp")
    return json.dumps(event)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, default=20, help="events per second")
    parser.add_argument("--hot-vehicle", action="store_true",
                        help=f"skew ~40%% of traffic onto {HOT_VEHICLE}")
    parser.add_argument("--unknown-vehicles", action="store_true",
                        help="~1%% of traffic from vehicles absent from the catalog")
    args = parser.parse_args()
    # `kill <pid>` (SIGTERM) stops the simulator as cleanly as Ctrl+C: buffered messages are flushed.
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    stats = {"delivered": 0, "failed": 0}

    def on_delivery(err, _msg):
        if err is None:
            stats["delivered"] += 1
        else:
            stats["failed"] += 1
            if stats["failed"] <= 5:
                print(f"delivery failed: {err}")

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "enable.idempotence": True,
        "linger.ms": 10,
        "batch.size": 32768,
    })

    def send(key: str, value: str):
        while True:
            try:
                producer.produce(TOPIC, key=key, value=value, on_delivery=on_delivery)
                return
            except BufferError:
                # local queue full (e.g. broker down): wait for deliveries, then retry
                producer.poll(0.5)

    vehicle_ids = [f"V{i:04d}" for i in range(1, NUM_VEHICLES + 1)]
    sustained = {}  # vehicle_id -> episode end (epoch seconds)
    seq = 0
    print(f"run {RUN_ID}: streaming ~{args.rate} events/s (hot vehicle: {args.hot_vehicle}, "
          f"unknown vehicles: {args.unknown_vehicles}). Ctrl+C to stop.")
    try:
        while True:
            tick = time.time()
            sustained = {v: end for v, end in sustained.items() if end > tick}
            if len(sustained) < SUSTAINED_MAX and random.random() < 1 / SUSTAINED_EVERY_S:
                v = random.choice([v for v in vehicle_ids if v not in sustained])
                sustained[v] = tick + random.uniform(*SUSTAINED_DURATION_S)
                print(f"sustained speeding episode started for {v}")
            for _ in range(args.rate):
                if args.hot_vehicle and random.random() < HOT_SHARE:
                    vehicle_id = HOT_VEHICLE
                elif args.unknown_vehicles and random.random() < UNKNOWN_RATE:
                    vehicle_id = random.choice(UNKNOWN_VEHICLES)
                else:
                    vehicle_id = random.choice(vehicle_ids)
                seq += 1
                event = make_event(vehicle_id, seq, vehicle_id in sustained)
                payload = corrupt(event) if random.random() < MALFORMED_RATE else json.dumps(event)
                send(vehicle_id, payload)
                if random.random() < DUPLICATE_RATE:
                    send(vehicle_id, payload)
            producer.poll(0)
            time.sleep(max(0.0, 1.0 - (time.time() - tick)))
    except KeyboardInterrupt:
        pass
    finally:
        remaining = producer.flush(30)
        print(f"stopped: {stats['delivered']} delivered, {stats['failed']} failed, "
              f"{remaining} still in the local queue")


if __name__ == "__main__":
    main()
