"""Synthetic event producer: clickstream + order events to Kafka.

Events are plain JSON on one topic so the streaming job has a single,
stable schema to parse. Run a local broker first (docker compose up).

Usage: python src/producer.py --bootstrap localhost:9092 --events 200
"""
import argparse, json, random, time, uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

PAGES = ["/home", "/products", "/products/P100", "/cart", "/checkout", "/search"]
PRODUCTS = ["P100", "P101", "P102", "P103", "P104", "P105"]

def make_event(rng: random.Random) -> dict:
    kind = rng.choices(["click", "order"], weights=[8, 2])[0]
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": kind,
        "user_id": f"U{rng.randint(1, 500):04d}",
        "event_time": datetime.now(timezone.utc).isoformat(),
    }
    if kind == "click":
        event["page"] = rng.choice(PAGES)
    else:
        event["product_id"] = rng.choice(PRODUCTS)
        event["quantity"] = rng.randint(1, 3)
        event["amount"] = round(rng.uniform(500, 16000), 2)
    return event

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="events.raw")
    parser.add_argument("--events", type=int, default=200)
    parser.add_argument("--interval", type=float, default=0.1,
                        help="seconds between events")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    rng = random.Random(7)
    for i in range(args.events):
        event = make_event(rng)
        producer.send(args.topic, value=event)
        if i % 50 == 0:
            print(f"sent {i} events")
        time.sleep(args.interval)
    producer.flush()
    producer.close()
    print(f"Done: {args.events} events -> topic {args.topic}")

if __name__ == "__main__":
    main()
