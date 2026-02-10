from confluent_kafka import Consumer, TopicPartition
import time

conf = {
    "bootstrap.servers": "106.52.208.15:9092",
    # "group.id": "timestamp-replay",
    "group.id": "order-service-manual",
    "enable.auto.commit": False,
}

c = Consumer(conf)

tp = TopicPartition("order-events", 0)

# 比如：2024-10-01 00:00:00
target_ts_ms = int(
    time.mktime(time.strptime("2024-05-01 00:00:00", "%Y-%m-%d %H:%M:%S"))
    * 1000
)

# target_ts_ms = 1770539906913

topic = "order-events"
# topic = "test-topic"

metadata = c.list_topics(topic, timeout=10)
partitions = metadata.topics[topic].partitions.keys()


tps = [
    TopicPartition(topic, p, target_ts_ms)
    for p in partitions
]

offsets = c.offsets_for_times(tps, timeout=10)

valid_tps = []
for tp in offsets:
    if tp.offset >= 0:
        valid_tps.append(tp)

c.assign(valid_tps)


try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(msg.error())
            continue

        print(
            msg.topic(),
            msg.partition(),
            msg.offset(),
            msg.timestamp(),
            msg.value()
        )

except KeyboardInterrupt:
    pass
finally:
    c.close()
