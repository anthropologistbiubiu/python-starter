

from confluent_kafka import Consumer

conf = {
    "bootstrap.servers": "106.52.208.15:9092",
    "group.id": "inspect-meta",
}

c = Consumer(conf)

md = c.list_topics("order-events-service", timeout=10)
topic = md.topics["order-events-service"]

for p_id, p in topic.partitions.items():
    print(
        f"partition={p_id}, "
        f"leader={p.leader}, "
        f"replicas={p.replicas}, "
        f"isr={p.isrs}"
    )
