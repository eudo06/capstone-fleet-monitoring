"""
Capstone Project -- create every Kafka topic used by the pipeline.

  vehicle_events      6 partitions   raw telemetry (keyed by vehicle_id)
  fleet_catalog       3 partitions   COMPACTED reference data (keyed by vehicle_id)
  alerts              3 partitions   anomaly alerts produced by Spark
  region_kpis_1min    3 partitions   1-minute regional KPIs produced by Spark
  vehicle_events_dlq  3 partitions   malformed events routed by Spark (dead-letter queue)
  daily_reports       1 partition    daily summaries published by Airflow
"""
import time

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVERS = "localhost:9092"

TOPICS = [
    NewTopic("vehicle_events", num_partitions=6, replication_factor=1),
    NewTopic("fleet_catalog", num_partitions=3, replication_factor=1,
             config={"cleanup.policy": "compact"}),
    NewTopic("alerts", num_partitions=3, replication_factor=1),
    NewTopic("region_kpis_1min", num_partitions=3, replication_factor=1),
    NewTopic("vehicle_events_dlq", num_partitions=3, replication_factor=1),
    NewTopic("daily_reports", num_partitions=1, replication_factor=1),
]


def main():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    for attempt in range(1, 7):  # the broker may still be starting after `docker compose up -d`
        try:
            existing = set(admin.list_topics(timeout=10).topics)
            break
        except KafkaException as exc:
            if attempt == 6:
                raise SystemExit(f"cannot reach Kafka at {BOOTSTRAP_SERVERS}: {exc}")
            print(f"broker not ready ({exc}), retrying in 5 s...")
            time.sleep(5)
    to_create = [t for t in TOPICS if t.topic not in existing]
    if not to_create:
        print("all topics already exist")
        return
    for name, fut in admin.create_topics(to_create).items():
        fut.result()
        print(f"created {name}")


if __name__ == "__main__":
    main()
