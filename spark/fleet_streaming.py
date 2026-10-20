import json
import time
from confluent_kafka import Consumer, TopicPartition
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import when, lit
from pyspark.sql.functions import from_json, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
    IntegerType
)
from pyspark.sql.functions import struct, to_json
from pyspark.sql.functions import broadcast, coalesce
from pyspark.sql.functions import concat_ws, sha2
from pyspark.sql.functions import window, count, avg, max, sum, when
from pathlib import Path
import shutil


spark = (
    SparkSession.builder
    .appName("FleetMonitoring")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

CATALOG_TOPIC = "fleet_catalog"

catalog_schema = StructType([
    StructField("vehicle_id", StringType(), False),
    StructField("plate", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("home_region", StringType(), True),
    StructField("max_payload_kg", IntegerType(), True),
])


def load_fleet_catalog(timeout_s=60):
    """Read the compacted topic from the beginning up to the end offsets observed at
    start-up and keep the latest value per key (a tombstone deletes the key).
    This is a snapshot: later catalog changes are not seen until the job restarts."""
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "fleet-catalog-loader",  # required by the client; nothing is committed
        "enable.auto.commit": False,
    })

    vehicles = {}

    try:
        metadata = consumer.list_topics(CATALOG_TOPIC, timeout=10)
        topic_meta = metadata.topics.get(CATALOG_TOPIC)
        if topic_meta is None or topic_meta.error is not None:
            raise RuntimeError(f"topic {CATALOG_TOPIC} not found: run scripts/create_topics.py")

        # End offset of each partition at start-up = what we must read.
        end_offsets = {}
        assignment = []
        for partition_id in topic_meta.partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(CATALOG_TOPIC, partition_id), timeout=10
            )
            if high > low:  # an empty partition is already "finished"
                end_offsets[partition_id] = high
                assignment.append(TopicPartition(CATALOG_TOPIC, partition_id, low))

        consumer.assign(assignment)
        pending = set(end_offsets)
        deadline = time.monotonic() + timeout_s

        while pending:
            if time.monotonic() > deadline:
                raise RuntimeError(f"catalog not fully read after {timeout_s}s, "
                                   f"partitions still pending: {sorted(pending)}")

            message = consumer.poll(1.0)
            if message is None:
                # Compaction can leave gaps in the offsets, so offset high-1 may never
                # arrive: compare the consumer position with the end offset instead.
                for tp in consumer.position(
                        [TopicPartition(CATALOG_TOPIC, p) for p in pending]):
                    if tp.offset >= end_offsets[tp.partition]:
                        pending.discard(tp.partition)
                continue
            if message.error():
                raise RuntimeError(message.error())

            partition = message.partition()
            if message.offset() < end_offsets[partition]:
                key = message.key()
                if key is None:
                    print(f"catalog: record without key at {partition}@{message.offset()}, ignored")
                elif message.value() is None:
                    vehicles.pop(key.decode("utf-8"), None)  # tombstone
                else:
                    vehicles[key.decode("utf-8")] = message.value()
            if message.offset() >= end_offsets[partition] - 1:
                pending.discard(partition)
    finally:
        consumer.close()

    # Validate each record explicitly (instead of filtering on the shape of vehicle_id).
    catalog = []
    for vehicle_id, raw in vehicles.items():
        try:
            record = json.loads(raw)
            if record["vehicle_id"] != vehicle_id:
                raise ValueError("key differs from vehicle_id")
            catalog.append((
                record["vehicle_id"],
                record["plate"],
                record["vehicle_type"],
                record["home_region"],
                int(record["max_payload_kg"]),
            ))
        except (ValueError, KeyError, TypeError) as exc:
            print(f"catalog: invalid record for key {vehicle_id!r} ignored ({exc!r})")
    return catalog


# Explicit schema: also works when the catalog is empty (schema inference would fail).
catalog_df = spark.createDataFrame(load_fleet_catalog(), schema=catalog_schema)

print("Nombre de véhicules dans le catalogue :", catalog_df.count())

catalog_df.show(5, truncate=False)


events_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "vehicle_events")
    .option("startingOffsets", "earliest")
    .load()
)

events_df.printSchema()

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("speed_kmh", DoubleType(), True),
    StructField("fuel_level_pct", DoubleType(), True),
    StructField("harsh_braking", BooleanType(), True),
    StructField("_corrupt_record", StringType(), True)
])


messages_df = events_df.select(
    col("key").cast("string").alias("vehicle_key"),
    col("value").cast("string").alias("raw_json"),
    col("partition"),
    col("offset")
)

parsed_df = messages_df.withColumn(
    "event",
    from_json(
        col("raw_json"),
        event_schema,
        {
            "mode": "PERMISSIVE",
            "columnNameOfCorruptRecord": "_corrupt_record"
        }
    )
)

parsed_df = parsed_df.select(
    "vehicle_key",
    "raw_json",
    "partition",
    "offset",
    "event.*"
)

parsed_df = parsed_df.withColumn(
    "event_timestamp",
    to_timestamp(
        col("event_time"),
        "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
    )
)

validated_df = parsed_df.withColumn(
    "error_reason",
    when(col("event_id").isNull() & col("vehicle_id").isNull(),lit("invalid_json"))
    .when(col("event_id").isNull(), lit("missing_event_id"))
    .when(col("vehicle_id").isNull(), lit("missing_vehicle_id"))
    .when(col("event_time").isNull(), lit("missing_event_time"))
    .when(col("event_timestamp").isNull(), lit("invalid_timestamp"))
    .when(col("latitude").isNull(), lit("missing_latitude"))
    .when(col("longitude").isNull(), lit("missing_longitude"))
    .when(col("speed_kmh").isNull(), lit("missing_speed"))
    .when(col("fuel_level_pct").isNull(), lit("missing_fuel_level"))
    .when(col("harsh_braking").isNull(), lit("missing_harsh_braking"))
    .when(
        (col("latitude") < -90) | (col("latitude") > 90),
        lit("invalid_latitude")
    )
    .when(
        (col("longitude") < -180) | (col("longitude") > 180),
        lit("invalid_longitude")
    )
    .when(
        (col("speed_kmh") < 0) | (col("speed_kmh") > 250),
        lit("invalid_speed")
    )
    .when(
        (col("fuel_level_pct") < 0) | (col("fuel_level_pct") > 100),
        lit("invalid_fuel_level")
    )
)

valid_df = validated_df.filter(
    col("error_reason").isNull()
)

invalid_df = validated_df.filter(
    col("error_reason").isNotNull()
)

enriched_before_dedup_df = (
    valid_df
    .join(
        broadcast(catalog_df),
        on="vehicle_id",
        how="left"
    )
    .withColumn(
        "home_region",
        coalesce(col("home_region"), lit("UNKNOWN"))
    )
)

deduplicated_df = (
    enriched_before_dedup_df
    .withWatermark("event_timestamp", "2 minutes")
    .dropDuplicatesWithinWatermark(["event_id"])
)

enriched_df = deduplicated_df

def process_batch(batch_df, batch_id):

    print(f"Processing batch: {batch_id}")

    parquet_batch_df = (
        batch_df
        .withColumn("event_date", col("event_timestamp").cast("date"))
        .withColumn("batch_id", lit(batch_id))
    )

    speeding_batch_df = (
        batch_df
        .filter(col("speed_kmh") > 120)
        .withColumn("alert_type", lit("SPEEDING"))
    )

    braking_batch_df = (
        batch_df
        .filter(col("harsh_braking") == True)
        .withColumn("alert_type", lit("HARSH_BRAKING"))
    )

    alerts_batch_df = speeding_batch_df.unionByName(braking_batch_df)

    kafka_batch_df = alerts_batch_df.select(
        col("vehicle_id").cast("string").alias("key"),

        to_json(
            struct(
                col("event_id"),
                col("vehicle_id"),
                col("event_time"),
                col("speed_kmh"),
                col("home_region"),
                col("alert_type")
            )
        ).alias("value")
    )

    (
        kafka_batch_df
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "alerts")
        .save()
    )

    base_path = Path("data/silver/vehicle_events_dedup_v2").resolve()

    final_path = base_path / f"batch_id={batch_id}"
    temp_path = base_path / f"_temporary_batch_{batch_id}"

    base_path.mkdir(parents=True, exist_ok=True)

    
    if not final_path.exists():

        if temp_path.exists():
            shutil.rmtree(temp_path)

        (
            parquet_batch_df
            .drop("batch_id")
            .coalesce(4)
            .write
            .mode("overwrite")
            .partitionBy("event_date")
            .parquet(str(temp_path))
        )

        temp_path.rename(final_path)

        print(f"Batch {batch_id} saved successfully")

    else:
        print(f"Batch {batch_id} already saved, skipping Parquet")

speeding_df = enriched_df.filter(
    col("speed_kmh") > 120
)

braking_df = enriched_df.filter(
    col("harsh_braking") == True
)


speeding_df = speeding_df.withColumn(
    "alert_type",
    lit("SPEEDING")
)

braking_df = braking_df.withColumn(
    "alert_type",
    lit("HARSH_BRAKING")
)


alerts_df = speeding_df.unionByName(braking_df)

kafka_alerts_df = alerts_df.select(
    col("vehicle_id").cast("string").alias("key"),

    to_json(
        struct(
            col("event_id"),
            col("vehicle_id"),
            col("event_time"),
            col("speed_kmh"),
            col("home_region"),
            col("alert_type")
        )
    ).alias("value")
)



parquet_df = enriched_df.withColumn(
    "event_date",
    col("event_timestamp").cast("date")
)

dlq_df = invalid_df.select(
    col("vehicle_key").cast("string").alias("key"),
    to_json(
        struct(
            col("raw_json"),
            col("error_reason"),
            col("partition").alias("source_partition"),
            col("offset").alias("source_offset")
        )
    ).alias("value")
)

region_kpis_df = (
    enriched_df
    .groupBy(
        window(col("event_timestamp"), "1 minute"),
        col("home_region")
    )
    .agg(
        count("*").alias("event_count"),
        avg("speed_kmh").alias("avg_speed"),
        max("speed_kmh").alias("max_speed"),
        sum(
            when(col("speed_kmh") > 120, 1).otherwise(0)
        ).alias("speeding_count")
    )
)

region_kpis_kafka_df = region_kpis_df.select(
    concat_ws(
        "_",
        col("home_region"),
        col("window.start").cast("string")
    ).alias("key"),

    to_json(
        struct(
            col("home_region").alias("region"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("event_count"),
            col("avg_speed"),
            col("max_speed"),
            col("speeding_count")
        )
    ).alias("value")
)

sustained_speed_df = (
    enriched_df
    .groupBy(
        window(
            col("event_timestamp"),
            "5 minutes",
            "1 minute"
        ),
        col("vehicle_id")
    )
    .agg(
        count("*").alias("event_count"),
        avg("speed_kmh").alias("avg_speed")
    )
)

sustained_alerts_df = (
    sustained_speed_df
    .filter(
        (col("event_count") >= 5) &
        (col("avg_speed") > 100)
    )
    .withColumn(
        "alert_type",
        lit("sustained_speeding")
    )
)

sustained_alerts_kafka_df = sustained_alerts_df.select(
    col("vehicle_id").cast("string").alias("key"),

    to_json(
        struct(
            col("vehicle_id"),
            col("alert_type"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("event_count"),
            col("avg_speed")
        )
    ).alias("value")
)

query1 = (
    enriched_df
    .writeStream
    .foreachBatch(process_batch)
    .outputMode("append")
    .option("checkpointLocation", "checkpoints/query1_dedup_v2")
    .start()
)


query2 = (
    region_kpis_kafka_df
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "region_kpis_1min")
    .option("checkpointLocation", "checkpoints/query2_dedup_v2")
    .outputMode("update")
    .start()
)

query3 = (
    sustained_alerts_kafka_df
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "alerts")
    .option("checkpointLocation", "checkpoints/query3_dedup_v2")
    .outputMode("append")
    .start()
)

query_dlq = (
    dlq_df
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "vehicle_events_dlq")
    .option("checkpointLocation", "checkpoints/query_dlq")
    .outputMode("append")
    .start()
)



spark.streams.awaitAnyTermination()