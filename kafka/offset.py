# consumer.py
from confluent_kafka import Consumer, TopicPartition

conf = {
    "bootstrap.servers": "106.52.208.15:9092",
    "group.id": "order-service-manual",
    # "group.id": "manual-offset-reader",
    "enable.auto.commit": False,
    "auto.offset.reset": "earliest"
}


def on_assign(consumer, partitions):
    for p in partitions:
        if p.topic == "order-events" and p.partition == 0:
            p.offset = 10   # 👈 在这里指定 offset
    consumer.assign(partitions)


c = Consumer(conf)
c.subscribe(["order-events"], on_assign=on_assign)

while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(msg.error())
        continue

    print(msg.partition(), msg.offset(), msg.key(), msg.value())
