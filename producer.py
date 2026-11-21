import json
import random
import time
from confluent_kafka import Producer

# Connect to the Kafka broker running in Docker
producer = Producer({"bootstrap.servers": "localhost:9092"})

TOPIC = "claims.new"
CLAIM_TYPES = ["auto", "property", "injury"]


def delivery_report(err, msg):
    """Called once Kafka confirms (or rejects) each message."""
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(
            f"✅ {msg.key().decode()} -> partition {msg.partition()}, "
            f"offset {msg.offset()}"
        )


for i in range(1, 11):
    claim_id = f"C-{1000 + i}"
    claim = {
        "claim_id": claim_id,
        "type": random.choice(CLAIM_TYPES),
        "amount": random.randint(500, 50000),
        "status": "Filed",
    }

    # Key = claim ID, so every event for the same claim lands in the same partition
    producer.produce(
        TOPIC,
        key=claim_id,
        value=json.dumps(claim),
        callback=delivery_report,
    )
    producer.poll(0)  # trigger delivery callbacks
    time.sleep(0.5)

producer.flush()  # wait until every message is confirmed
print("Done sending 10 claims.")