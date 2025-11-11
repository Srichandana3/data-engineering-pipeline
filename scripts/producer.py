from kafka import KafkaProducer
import json, time, random, uuid, datetime as dt

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("✅ Producing messages to 'transactions' topic. Press Ctrl+C to stop.")

while True:
    event = {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": random.randint(1000, 9999),
        "amount": round(random.uniform(5, 900), 2),
        "merchant": random.choice(["Amazon", "Walmart", "BestBuy", "Costco"]),
        "device": random.choice(["iPhone", "Android", "Web", "POS Terminal"]),
        "timestamp": int(time.time()),
        "timestamp_iso": dt.datetime.utcnow().isoformat() + "Z"
    }
    
    producer.send("transactions", event)
    producer.flush()
    
    print("→ Sent:", event)
    time.sleep(1)
