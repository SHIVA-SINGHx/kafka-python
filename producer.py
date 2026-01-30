import uuid
import json
from confluent_kafka import Producer

producer_config = Producer({'bootstrap.servers': 'localhost:9092'})

producer = Producer(producer_config)

def report_log(err, msg):
    if err:
        print(f"Delivered Failed Error : {err}")
        
    else:
        print(f"Delivered Successfully : {msg.value().decode("utf-8")}")
        print(f"Delivered Successfullt {msg.topic()}: Partition: {msg.partition()}: offset {msg.offset()}" )
    
    
    
order = {
    "order__id": str(uuid.uuid4()),
    "user": "shiva",
    "item": "butter_chicken",
    "quantiy": 4,
}


value = json.dumps(order).encode("utf-8")  ## this line will convert into json to byte format cu'z kafka understand byte format

producer.produce(topic=order, value=value, callback=report_log)

producer.flush()


