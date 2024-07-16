import time
import random
import json
from confluent_kafka import Producer

kafka_config = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(kafka_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

for i in range(100):
    product = {
        "id": i,
        "name": f"Product {i}",
        "description": f"Description for product {i}",
        "price": round(random.uniform(50, 70), 2),
    }
    producer.produce(
        topic="products_5",
        key=str(i),
        value=json.dumps(product),
        on_delivery=delivery_report
    )

    for _ in range(random.randint(1, 10)) :
        purchase = {
            "id" : random.randint(0, 1000000),
            "quantity" : random.randint(1, 11),
            "product_id" : i,
            "timestamp" : int(time.time())
        }
        producer.produce(
            topic="purchases_5",
            key=str(purchase["id"]),
            value=json.dumps(purchase),
            on_delivery=delivery_report
        )

    producer.flush()

print("Done!")
