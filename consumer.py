import json
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "claims-reader2",       # consumers in the same group share the work
    "auto.offset.reset": "earliest",   # new group? start from the beginning
})

consumer.subscribe(["claims.new"])
print("Listening for claims... press Ctrl+C to stop.")

try:
    while True:
        msg = consumer.poll(1.0)  # wait up to 1 second for a message
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        claim = json.loads(msg.value())
        print(
            f"📥 partition {msg.partition()} | offset {msg.offset()} | "
            f"{claim['claim_id']} | {claim['type']} | ${claim['amount']:,}"
        )
except KeyboardInterrupt:
    pass
finally:
    consumer.close()  # commits offsets and leaves the group cleanly