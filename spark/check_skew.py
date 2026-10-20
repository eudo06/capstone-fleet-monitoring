from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc

spark = (
    SparkSession.builder
    .appName("CheckDataSkew")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(
    "data/silver/vehicle_events_dedup_v2"
)

daily_df = df.filter(
    col("event_date") == "2026-10-18"
)

print("\n=== TOTAL EVENTS ===")

total_events = daily_df.count()
print(f"Total events: {total_events}")

print("\n=== EVENTS PER VEHICLE ===")

vehicle_counts = (
    daily_df
    .groupBy("vehicle_id")
    .agg(count("*").alias("events"))
    .orderBy(desc("events"))
)

vehicle_counts.show(10, truncate=False)

print("\n=== V0001 PERCENTAGE ===")

hot_events = daily_df.filter(
    col("vehicle_id") == "V0001"
).count()

hot_percentage = (
    hot_events / total_events * 100
    if total_events > 0 else 0
)

print(f"V0001 events: {hot_events}")
print(f"V0001 percentage: {hot_percentage:.2f}%")

spark.stop()