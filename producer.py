import uuid
import json
from confluent_kafka import Producer

producer = Producer({
    'bootstrap.servers': 'localhost:9092'
})

def report_log(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered to {msg.topic()} | "
            f"Partition {msg.partition()} | "
            f"Offset {msg.offset()}"
        )

order = {
    "order_id": str(uuid.uuid4()),
    "user": "shiva",
    "item": "butter_chicken",
    "quantity": 4,
}

value = json.dumps(order).encode("utf-8")

producer.produce(
    topic="orders",  
    value=value,
    callback=report_log
)

producer.flush()

