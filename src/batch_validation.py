"""Batch sanity checks over the streamed Delta table.

Run after the streaming job has processed some events: verifies the
table is readable, counts events by type, and flags null keys or
negative amounts that should never survive parsing.
"""
import argparse
from pyspark.sql import functions as F
from stream_to_delta import get_spark

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="data/delta/events")
    args = parser.parse_args()
    spark = get_spark("stream-validation")

    df = spark.read.format("delta").load(args.target)
    total = df.count()
    print(f"Total events: {total}")
    df.groupBy("event_type").count().orderBy("event_type").show(truncate=False)

    problems = {
        "null event_id": df.filter(F.col("event_id").isNull()).count(),
        "null event_time": df.filter(F.col("event_time").isNull()).count(),
        "negative amount": df.filter(F.col("amount") < 0).count(),
    }
    for name, count in problems.items():
        print(f"{name}: {count}")
    bad = sum(problems.values())
    if bad:
        raise SystemExit(f"Validation FAILED: {bad} problem rows")
    print("Validation passed.")
    spark.stop()

if __name__ == "__main__":
    main()
