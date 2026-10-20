import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.functions import spark_partition_id
from pyspark.sql.functions import when, pmod, xxhash64, lit
from pyspark.sql.functions import sum as spark_sum

spark = (
    SparkSession.builder
    .appName("FleetSkewBaseline")
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

print("\n=== BASELINE AGGREGATION ===")

start_time = time.perf_counter()

result = (
    daily_df
    .groupBy("vehicle_id")
    .agg(count("*").alias("events"))
)

print("\n=== BASELINE PHYSICAL PLAN ===")
result.explain(True)

result.show(50, truncate=False)

end_time = time.perf_counter()

print(f"\nExecution time: {end_time - start_time:.2f} seconds")

print("\n=== DATA DISTRIBUTION AFTER SHUFFLE ===")

partitioned_df = daily_df.repartition(8, "vehicle_id")

partition_distribution = (
    partitioned_df
    .withColumn("partition_id", spark_partition_id())
    .groupBy("partition_id")
    .count()
    .orderBy("partition_id")
)

partition_distribution.show(8, truncate=False)

print("\n=== SALTING EXPERIMENT ===")

NUM_SALTS = 8

salted_df = daily_df.withColumn(
    "salt",
    when(
        col("vehicle_id") == "V0001",
        pmod(xxhash64(col("event_id")), lit(NUM_SALTS))
    ).otherwise(lit(0))
)

salted_df.filter(
    col("vehicle_id") == "V0001"
).groupBy("salt").count().orderBy("salt").show()

print("\n=== TWO-STAGE SALTED AGGREGATION ===")

start_salted = time.perf_counter()

# Stage 1: partial aggregation by vehicle and salt
partial_counts = (
    salted_df
    .groupBy("vehicle_id", "salt")
    .agg(count("*").alias("partial_events"))
)

# Stage 2: final aggregation by vehicle
salted_result = (
    partial_counts
    .groupBy("vehicle_id")
    .agg(spark_sum("partial_events").alias("events"))
)

salted_result.show(50, truncate=False)

end_salted = time.perf_counter()

print(
    f"\nSalted execution time: "
    f"{end_salted - start_salted:.2f} seconds"
)

input("\nPress Enter to stop Spark...")



spark.stop()