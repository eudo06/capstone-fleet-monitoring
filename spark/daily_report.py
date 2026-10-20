import sys

from pyspark.sql.functions import desc
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    max,
    sum as spark_sum,
    when
)

spark = (
    SparkSession.builder
    .appName("FleetDailyReport")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


if len(sys.argv) != 2:
    print("Usage: python spark/daily_report.py YYYY-MM-DD")
    sys.exit(1)

target_date = sys.argv[1]

print(f"Generating daily report for: {target_date}")


parquet_path = "data/silver/vehicle_events_dedup_v2"

df = spark.read.parquet(parquet_path)

df.printSchema()

daily_df = df.filter(
    col("event_date") == target_date
)

print("\n=== SPARK EXECUTION PLAN ===")

daily_df.explain(True)


vehicle_report = (
    daily_df
    .groupBy("vehicle_id")
    .agg(
        count("*").alias("events"),
        avg("speed_kmh").alias("mean_speed"),
        max("speed_kmh").alias("maximum_speed"),
        spark_sum(
            when(col("speed_kmh") > 120, 1).otherwise(0)
        ).alias("speeding_events"),
        spark_sum(
            when(col("harsh_braking") == True, 1).otherwise(0)
        ).alias("harsh_braking_events")
    )
)

print("\n=== DAILY REPORT BY VEHICLE ===")

vehicle_report.orderBy("vehicle_id").show(10, truncate=False)

region_report = (
    daily_df
    .groupBy("home_region")
    .agg(
        count("*").alias("events"),
        avg("speed_kmh").alias("mean_speed"),
        max("speed_kmh").alias("maximum_speed"),
        spark_sum(
            when(col("speed_kmh") > 120, 1).otherwise(0)
        ).alias("speeding_events"),
        spark_sum(
            when(col("harsh_braking") == True, 1).otherwise(0)
        ).alias("harsh_braking_events")
    )
)

print("\n=== DAILY REPORT BY REGION ===")

region_report.orderBy("home_region").show(truncate=False)

worst_vehicles = (
    vehicle_report
    .withColumn(
        "risk_score",
        col("speeding_events") + col("harsh_braking_events")
    )
    .orderBy(
        desc("risk_score"),
        desc("speeding_events"),
        desc("maximum_speed")
    )
    .limit(5)
)

print("\n=== TOP 5 WORST VEHICLES ===")

worst_vehicles.show(truncate=False)


output_path = f"data/daily_reports/{target_date}"

vehicle_report.write \
    .mode("overwrite") \
    .parquet(f"{output_path}/vehicles")

region_report.write \
    .mode("overwrite") \
    .parquet(f"{output_path}/regions")

worst_vehicles.write \
    .mode("overwrite") \
    .parquet(f"{output_path}/worst_vehicles")

print(f"\nDaily reports saved to: {output_path}")

spark.stop()