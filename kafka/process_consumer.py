from confluent_kafka import Consumer
import json
import os

conf = {
    "bootstrap.servers": "106.52.208.15:9092",
    "group.id": "order-service",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True
}

c = Consumer(conf)
c.subscribe(["order-events-service"])

print(f"🟢 consumer started pid={os.getpid()}")

try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("❌", msg.error())
            continue

        data = json.loads(msg.value())
        print(
            f"[pid={os.getpid()}] "
            f"partition={msg.partition()} "
            f"offset={msg.offset()} "
            f"key={msg.key().decode()} "
            f"data={data}"
        )

except KeyboardInterrupt:
    pass
finally:
    c.close()
    print("⏹ consumer closed")
