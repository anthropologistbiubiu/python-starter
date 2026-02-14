

from confluent_kafka.admin import AdminClient, NewTopic


conf = {
    "bootstrap.servers": "106.52.208.15:9092"
}

admin = AdminClient(conf)


"""
admin.create_partitions({
    "order-events": NewPartitions(total_count=8)
})

"""


topic = NewTopic(
    topic="order-events-service",
    num_partitions=4,          # 👈 在这里指定
    replication_factor=1
)

fs = admin.create_topics([topic])

for topic, f in fs.items():
    try:
        f.result()
        print(f"✅ topic {topic} created")
    except Exception as e:
        print(f"❌ failed to create topic {topic}: {e}")
