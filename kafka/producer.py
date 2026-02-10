# producer.py
from confluent_kafka import Producer
import json
import time
import uuid

conf = {
    "bootstrap.servers": "106.52.208.15:9092",
    "acks": "all"
}

p = Producer(conf)


def delivery_report(err, msg):
    if err:
        print("Delivery failed:", err)
    else:
        print(
            f"Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


for i in range(5):
    event = {
        "event_id": str(uuid.uuid4()),
        "order_id": f"O{i}",
        "ts": int(time.time() * 1000)
    }
    # partition = hash(i) % partition_count
    p.produce(
        topic="order-events",
        key=event["order_id"],
        value=json.dumps(event),
        on_delivery=delivery_report
    )

p.flush()
