# consumer_to_minio.py
import time
from datetime import datetime
from io import BytesIO
from kafka import KafkaConsumer
import boto3

TOPIC = "transactions"

# From HOST to MinIO use localhost:9000
S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS = "minioadmin"
S3_SECRET = "minioadmin"
BUCKET = "raw"   # make sure this exists in MinIO

def key_for_now():
    now = datetime.utcnow()
    # path: streaming/YYYY/MM/DD/HH/<millis>.json
    return f"streaming/{now:%Y/%m/%d/%H}/{int(time.time()*1000)}.json"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda b: b.decode("utf-8")  # keep raw JSON text
)

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS,
    aws_secret_access_key=S3_SECRET,
)

print("Consuming from Kafka and writing to MinIO...")
for msg in consumer:
    data = msg.value.strip()
    key = key_for_now()
    s3.put_object(Bucket=BUCKET, Key=key, Body=BytesIO(data.encode("utf-8")))
    print("wrote s3://%s/%s -> %s" % (BUCKET, key, data[:140]))
