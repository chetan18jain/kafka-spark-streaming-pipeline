# Kafka + Spark Structured Streaming to Delta Lake

A personal portfolio project: real-time ingestion of synthetic
clickstream/order events from **Kafka** into **Delta Lake** with
**Spark Structured Streaming** — the streaming counterpart to the batch
medallion pipeline in `azure-medallion-lakehouse-pipeline`.

## Architecture

```
producer.py                 Kafka topic            stream_to_delta.py
(synthetic events)  ----->  events.raw    ----->   parse JSON (explicit schema)
                                                    watermark 10 min
                                                    dedupe on event_id
                                                         |
                                                         v
                                              Delta table (data/delta/events)
                                              + checkpoint (data/checkpoints)
                                                         |
                                              batch_validation.py (counts/QA)
```

## Run it locally

```bash
docker compose up -d                      # single-broker Kafka on :9092
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python src/producer.py --events 200       # terminal 1: emit events
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0 \
    src/stream_to_delta.py                # terminal 2: stream to Delta
python src/batch_validation.py            # terminal 3: verify the table
```

## Design notes

- **Checkpointing** — the streaming query writes offsets and state to
  `data/checkpoints/events`. On restart it resumes exactly where it
  stopped instead of re-reading the topic, and the checkpoint is the
  single source of recovery truth: delete the Delta table without the
  checkpoint (or vice versa) and the pair is inconsistent.
- **Exactly-once** — Kafka offsets in the checkpoint + Delta's
  transactional appends mean each micro-batch is committed once; a
  crash between micro-batches replays uncommitted offsets only.
  `dropDuplicates` on `event_id` additionally absorbs producer retries,
  which are at-least-once by nature.
- **Late-arriving data** — `withWatermark("event_time", "10 minutes")`
  tells Spark how long to keep state for an event-time window: events
  up to 10 minutes late are accepted, later ones are dropped, and state
  older than the watermark is evicted so the job can run indefinitely.
- **Explicit schema** — events are parsed with a declared `StructType`
  rather than schema inference, so a malformed producer change fails
  loudly instead of silently nulling columns.
- **Batch validation** — `src/batch_validation.py` re-reads the Delta
  table in batch mode and fails on null keys or negative amounts: the
  same table serves both the stream and batch QA, which is the
  lakehouse point.

## Layout

```
docker-compose.yml        single-broker Kafka (KRaft, no ZooKeeper)
src/producer.py           synthetic clickstream/order events
src/stream_to_delta.py    Kafka -> Delta streaming job
src/batch_validation.py   batch counts + quality checks over the table
```
