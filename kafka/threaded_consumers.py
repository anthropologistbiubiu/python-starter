# threaded_consumers.py
import json
import os
import signal
import threading
import time
from confluent_kafka import Consumer, KafkaError

BOOTSTRAP_SERVERS = "106.52.208.15:9092"
TOPIC = "order-events-service"
GROUP_ID = "order-service-threaded"

RUNNING = True


def start_consumer(worker_id: int):
    """
    单个 Consumer 线程
    """
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        # 为了看得清楚每个 consumer
        "client.id": f"consumer-{worker_id}",
    }

    c = Consumer(conf)
    c.subscribe([TOPIC])

    print(f"🟢 worker-{worker_id} started (pid={os.getpid()})")

    try:
        while RUNNING:
            msg = c.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ worker-{worker_id} error:", msg.error())
                    continue

            key = msg.key().decode() if msg.key() else None
            value = json.loads(msg.value().decode())

            print(
                f"[worker-{worker_id}] "
                f"partition={msg.partition()} "
                f"offset={msg.offset()} "
                f"key={key} "
                f"value={value}"
            )

    except KeyboardInterrupt:
        pass
    finally:
        c.close()
        print(f"🧹 worker-{worker_id} closed")


def shutdown(signum, frame):
    global RUNNING
    RUNNING = False
    print("\n🛑 shutdown signal received")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    THREAD_COUNT = 4  # 👈 你想要的“consumer 数量”

    threads = []

    for i in range(THREAD_COUNT):
        t = threading.Thread(target=start_consumer, args=(i,), daemon=True)
        t.start()
        threads.append(t)

    # 主线程阻塞
    while RUNNING:
        time.sleep(1)

    print("✅ main process exiting")
