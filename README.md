# Capstone project -- provided material

Read the subject (`sujet_projet_final.pdf`, one level up) first. This folder contains only what
the subject says is provided: a Kafka broker, the topic-creation script and the telemetry
simulator. Everything else (catalog publisher, notifier, Spark jobs, Airflow DAGs) is yours to write.

## Prerequisites
* Docker (Docker Desktop on Windows/macOS, or Docker Engine on Linux). On Windows, use WSL2.
* Python 3.10 or 3.11.
* Java 11 or 17 (needed by Spark): check with `java -version`.
* Airflow is NOT in `requirements.txt`: install it in its own virtual environment, with the
  constraints file, as in Practical Work 5.

## Start
    docker compose up -d                      # Kafka broker (KRaft mode) on localhost:9092
    python3 -m venv .venv && source .venv/bin/activate
    pip install -r requirements.txt           # also gives you `spark-submit` inside the venv
    python3 scripts/create_topics.py          # creates the six topics
    python3 producers/produce_vehicle_events.py --rate 20
    # options: --hot-vehicle (key skew, Part C.3), --unknown-vehicles (vehicles absent from the catalog)

Stop the simulator with Ctrl+C. `docker compose down` stops the broker (add `-v` to erase its data).

Look at a topic:

    docker exec capstone-kafka /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 --topic vehicle_events --from-beginning --max-messages 5

## Topics
| Topic | Partitions | Role |
|---|---|---|
| vehicle_events | 6 | raw telemetry, key = vehicle_id |
| fleet_catalog | 3, compacted | reference data, key = vehicle_id (you publish it) |
| alerts | 3 | alerts produced by your Spark job and your DAGs |
| region_kpis_1min | 3 | 1-minute regional indicators |
| vehicle_events_dlq | 3 | invalid messages, with a reason |
| daily_reports | 1 | daily report published by Airflow |

## Message formats
Telemetry (JSON, key = vehicle_id): `event_id`, `vehicle_id`, `event_time` (ISO 8601, UTC),
`latitude`, `longitude`, `speed_kmh`, `fuel_level_pct`, `harsh_braking`. The 40 vehicles are
`V0001` to `V0040`. `event_time` always has millisecond precision and an explicit offset, e.g.
`2026-10-13T09:16:00.123+00:00` (Spark pattern `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`). `event_id` is
unique per simulator run (it contains a run id), so restarting the simulator creates no false duplicates.

Catalog record (JSON, key = vehicle_id): `vehicle_id`, `plate`, `vehicle_type`, `home_region`,
`max_payload_kg`. Regions: Paris, Lyon, Marseille, Lille, Toulouse. Types: van, truck,
refrigerated_truck, motorbike. Weights: 500, 1000, 1500 or 3000 kg. You must generate these 40
records yourself (the `faker` package is installed if you want plates).

## What the simulator does on purpose
* About 3% anomalies: half with a speed above 120 km/h, half with `harsh_braking = true`
  (both must raise a point alert).
* Sustained speeding: every few minutes one vehicle drives at 101-118 km/h for 6 to 9 minutes
  (it must raise a `sustained_speeding` alert in Query 3, without point alerts).
* 1% duplicated messages (same `event_id`, sent right after the original).
* 2% late messages (event time 3 to 6 minutes in the past).
* 1% corrupted messages: broken JSON, missing field (`vehicle_id`, `event_time` or `speed_kmh`),
  impossible speed (-12 or 999), unparsable timestamp.
* With `--hot-vehicle`, about 40% of all traffic comes from V0001 (key skew).
* With `--unknown-vehicles`, about 1% of traffic comes from V0041-V0045, which are not in the
  catalog (they must get the region `UNKNOWN`).

Do not modify the simulator to hide these faults: your platform must handle them. At the end
(Ctrl+C) the simulator prints how many messages were delivered and how many failed.

## Suggested layout of your own project
    producers/      your catalog publisher
    consumers/      your alerts notifier
    spark_jobs/     streaming job, quality job, daily report job
    airflow_dags/   fleet_daily_pipeline, kafka_lag_monitor
    README.md       how to start everything (graded: it must work from `docker compose up`)

## Common problems
* *Cannot connect to localhost:9092*: Docker is not running, or the container is not up yet
  (`docker ps`, wait 15 seconds after `docker compose up -d`).
* *Broker restarted, data gone*: the data lives in the `kafka-data` volume; only
  `docker compose down -v` erases it.
* *Port 9092 already used*: stop the other Kafka, or change the port in both listener settings.
* *Spark cannot find the Kafka source*: add
  `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` to `spark-submit`.
* *Java not found*: install a JDK 11 or 17 and make sure `java -version` works in the same terminal.
