
from confluent_kafka import Consumer, TopicPartition

conf = {
    "bootstrap.servers": "106.52.208.15:9092",
    # "group.id": "inspect",
    "group.id": "order-service",
    # "group.id": "manual-offset-reader",
}

c = Consumer(conf)

tp = TopicPartition("order-events", 0)

low, high = c.get_watermark_offsets(tp, timeout=10)

print("earliest offset:", low)
print("latest offset:", high)
