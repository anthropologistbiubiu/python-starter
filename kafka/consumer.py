# consumer.py
from confluent_kafka import Consumer

conf = {
    "bootstrap.servers": "106.52.208.15:9092",
    "group.id": "order-service",
    "auto.offset.reset": "earliest"
}

c = Consumer(conf)
c.subscribe(["order-events"])

while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(msg.error())
        continue

    print(msg.partition(), msg.offset(), msg.key(), msg.value())
