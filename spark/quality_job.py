import sys
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.functions import sum as spark_sum

MAX_DUPLICATES = 0
MAX_UNKNOWN_PERCENTAGE = 15.0
MIN_ROWS = 1

spark = (
    SparkSession.builder
    .appName("FleetDataQuality")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

if len(sys.argv) != 2:
    print("Usage: python spark/quality_job.py YYYY-MM-DD")
    sys.exit(1)

target_date = sys.argv[1]

print(f"Analyzing data for: {target_date}")


parquet_path = "data/silver/vehicle_events_dedup_v2"

df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet(parquet_path)
)

daily_df = df.filter(
    to_date(col("event_timestamp")) == target_date
)


total_rows = daily_df.count()

duplicates_df = (
    daily_df
    .groupBy("event_id")
    .count()
    .filter(col("count") > 1)
)

duplicate_count = (
    duplicates_df
    .select(spark_sum(col("count") - 1).alias("duplicates"))
    .collect()[0]["duplicates"]
)

duplicate_count = duplicate_count or 0

unknown_count = daily_df.filter(
    col("home_region") == "UNKNOWN"
).count()

print(f"Unknown region events: {unknown_count}")

if total_rows > 0:
    unknown_percentage = (unknown_count / total_rows) * 100
else:
    unknown_percentage = 0.0

print(f"Unknown region percentage: {unknown_percentage:.2f}%")

print(f"Duplicated events: {duplicate_count}")
print(f"Total rows for {target_date}: {total_rows}")


reasons = []

if total_rows < MIN_ROWS:
    reasons.append("No events found for the selected date")

if duplicate_count > MAX_DUPLICATES:
    reasons.append("Duplicated event IDs detected")

if unknown_percentage > MAX_UNKNOWN_PERCENTAGE:
    reasons.append("UNKNOWN region percentage exceeds 15%")

passed = len(reasons) == 0

verdict = {
    "date": target_date,
    "total_rows": total_rows,
    "duplicate_event_ids": duplicate_count,
    "unknown_region_count": unknown_count,
    "unknown_region_percentage": round(unknown_percentage, 4),
    "passed": passed,
    "reasons": reasons
}



daily_df.select(
    "event_id",
    "vehicle_id",
    "event_timestamp",
    "home_region"
).show(5, truncate=False)

output_dir = "data/quality_reports"

os.makedirs(output_dir, exist_ok=True)

output_path = f"{output_dir}/quality_{target_date}.json"

with open(output_path, "w") as f:
    json.dump(verdict, f, indent=4)

print(f"Quality report saved to: {output_path}")
print(json.dumps(verdict, indent=4))

spark.stop()