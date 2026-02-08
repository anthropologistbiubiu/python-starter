

from kafka import KafkaConsumer
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='106.52.208.15:9092')
producer.send('test-topic', b'hello from laptop')
producer.send('test-topic', b'hello world')
producer.flush()


consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers='106.52.208.15:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group'
)

for msg in consumer:
    print(f"[{msg.topic}] {msg.value}")
