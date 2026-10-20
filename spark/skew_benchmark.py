import time
import statistics

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    when,
    pmod,
    xxhash64,
    lit
)


spark = (
    SparkSession.builder
    .appName("FleetSkewBenchmark")
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

def baseline_aggregation(df):
    return (
        df.groupBy("vehicle_id")
        .agg(count("*").alias("events"))
    )

def salted_aggregation(df):

    salted_df = df.withColumn(
        "salt",
        when(
            col("vehicle_id") == "V0001",
            pmod(xxhash64(col("event_id")), lit(8))
        ).otherwise(lit(0))
    )

    partial_counts = (
        salted_df
        .groupBy("vehicle_id", "salt")
        .agg(count("*").alias("partial_events"))
    )

    return (
        partial_counts
        .groupBy("vehicle_id")
        .agg(spark_sum("partial_events").alias("events"))
    )

experiments = [
    ("Baseline", baseline_aggregation, False),
    ("Salting", salted_aggregation, False),
    ("AQE", baseline_aggregation, True)
]

def run_experiment(name, aggregation_function, aqe_enabled):

    spark.conf.set(
        "spark.sql.adaptive.enabled",
        str(aqe_enabled).lower()
    )

    start_time = time.perf_counter()

    result = aggregation_function(daily_df).collect()

    execution_time = time.perf_counter() - start_time

    results_dict = {
        row["vehicle_id"]: row["events"]
        for row in result
    }

    return execution_time, results_dict


print("\n=== WARM-UP ===")

reference_results = None

for name, function, aqe in experiments:

    duration, results = run_experiment(name, function, aqe)

    if reference_results is None:
        reference_results = results
    else:
        assert results == reference_results, (
            f"Results mismatch for {name}"
        )

    print(f"{name}: {duration:.3f} seconds")



print("\n=== BENCHMARK ===")

measurements = {
    "Baseline": [],
    "Salting": [],
    "AQE": []
}

for iteration in range(5):

    print(f"\nIteration {iteration + 1}")

    # Alternate execution order between iterations
    current_experiments = (
        experiments
        if iteration % 2 == 0
        else list(reversed(experiments))
    )

    for name, function, aqe in current_experiments:

        duration, results = run_experiment(name, function, aqe)

        assert results == reference_results, (
            f"Results mismatch for {name}"
        )

        measurements[name].append(duration)

        print(f"{name}: {duration:.3f} seconds")


print("\n=== FINAL BENCHMARK RESULTS ===")

for name, times in measurements.items():

    median_time = statistics.median(times)
    mean_time = statistics.mean(times)

    print(
        f"{name}: "
        f"mean={mean_time:.3f}s, "
        f"median={median_time:.3f}s"
    )

input("\nPress Enter to stop Spark...")

spark.stop()