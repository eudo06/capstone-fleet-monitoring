import json
from confluent_kafka import Producer


producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "enable.idempotence": True,
    "acks": "all"
})

regions = [
    "Île-de-France",
    "Normandie",
    "Bretagne",
    "Occitanie",
    "Auvergne-Rhône-Alpes"
]

vehicles = []

for i in range(1, 41):
    vehicle = {
        "vehicle_id": f"V{i:04d}",
        "plate": f"AB-{i:03d}-CD",
        "vehicle_type": "van",
        "home_region": regions[(i - 1) % len(regions)],
        "max_payload_kg": 1200
    }

    vehicles.append(vehicle)

def delivery_report(err, msg):
    if err is not None:
        print(f"Erreur de livraison : {err}")
    else:
        print(
            f"Message livré dans {msg.topic()}, "
            f"partition {msg.partition()}, "
            f"offset {msg.offset()}"
        )

for vehicle in vehicles:
    producer.produce(
        topic="fleet_catalog",
        key=vehicle["vehicle_id"],
        value=json.dumps(vehicle).encode("utf-8"),
        callback=delivery_report
    )

remaining = producer.flush(30)

if remaining > 0:
    print(f"Attention : {remaining} message(s) non livré(s).")

producer.flush()