import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    spark_partition_id,
    when,
    pmod,
    xxhash64,
    lit
)

spark = (
    SparkSession.builder
    .appName("FleetSkewTaskAnalysis")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(
    "data/silver/vehicle_events_dedup_v2"
)

daily_df = df.filter(
    col("event_date") == "2026-10-18"
)

print("\n=== BASELINE PARTITION DISTRIBUTION ===")

baseline_df = daily_df.repartition(
    8,
    "vehicle_id"
)

baseline_distribution = (
    baseline_df
    .withColumn(
        "partition_id",
        spark_partition_id()
    )
    .groupBy("partition_id")
    .agg(count("*").alias("events"))
    .orderBy("partition_id")
)

baseline_distribution.show(8, truncate=False)

print("\n=== SALTED PARTITION DISTRIBUTION ===")

NUM_SALTS = 8

salted_df = daily_df.withColumn(
    "salt",
    when(
        col("vehicle_id") == "V0001",
        pmod(
            xxhash64(col("event_id")),
            lit(NUM_SALTS)
        )
    ).otherwise(lit(0))
)

salted_partitioned_df = salted_df.repartition(
    8,
    "vehicle_id",
    "salt"
)


salted_distribution = (
    salted_partitioned_df
    .withColumn(
        "partition_id",
        spark_partition_id()
    )
    .groupBy("partition_id")
    .agg(count("*").alias("events"))
    .orderBy("partition_id")
)

salted_distribution.show(8, truncate=False)

print("\n=== SPARK UI ===")
print("Open http://localhost:4040")

input("\nPress Enter to stop Spark...")

spark.stop()