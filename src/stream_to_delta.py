"""Spark Structured Streaming: Kafka -> Delta Lake.

Reads JSON events from Kafka, parses them against an explicit schema,
applies a watermark so late events are handled deterministically, and
writes to Delta with checkpointing. Delta + checkpoint together give
exactly-once end to end: a restart resumes from the checkpoint and
Delta transaction log prevents duplicate commits.

Usage:
  spark-submit src/stream_to_delta.py --bootstrap localhost:9092
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, LongType, StringType,
                               StructField, StructType, TimestampType)

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("user_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("page", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", LongType()),
    StructField("amount", DoubleType()),
])

def get_spark(app_name: str) -> SparkSession:
    return (SparkSession.builder.appName(app_name)
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())

def build_stream(spark, bootstrap: str, topic: str,
                 target: str, checkpoint: str):
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", topic)
           .option("startingOffsets", "latest")
           .load())
    events = (raw.select(F.from_json(F.col("value").cast("string"),
                                     EVENT_SCHEMA).alias("e"))
              .select("e.*")
              # Drop events more than 10 minutes late relative to the
              # watermark; state for aggregations can then be cleaned up.
              .withWatermark("event_time", "10 minutes")
              .dropDuplicates(["event_id"]))
    return (events.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .option("path", target)
            .trigger(processingTime="30 seconds")
            .start())

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="events.raw")
    parser.add_argument("--target", default="data/delta/events")
    parser.add_argument("--checkpoint", default="data/checkpoints/events")
    args = parser.parse_args()
    spark = get_spark("kafka-to-delta")
    query = build_stream(spark, args.bootstrap, args.topic,
                         args.target, args.checkpoint)
    print(f"Streaming {args.topic} -> {args.target} (checkpoint {args.checkpoint})")
    query.awaitTermination()

if __name__ == "__main__":
    main()
