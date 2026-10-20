import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = (
    SparkSession.builder
    .appName("FleetSkewAQE")
    .config("spark.sql.adaptive.enabled", "true")
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

print("\n=== AQE AGGREGATION ===")

start_time = time.perf_counter()

result = (
    daily_df
    .groupBy("vehicle_id")
    .agg(count("*").alias("events"))
)

result.show(50, truncate=False)

end_time = time.perf_counter()

print(f"\nAQE execution time: {end_time - start_time:.2f} seconds")

print("\n=== AQE PHYSICAL PLAN ===")
result.explain(True)

input("\nPress Enter to stop Spark...")

spark.stop()