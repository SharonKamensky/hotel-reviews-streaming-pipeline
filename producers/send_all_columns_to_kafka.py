import pandas as pd
from kafka import KafkaProducer
import json
import time
import os

# Relative path into data/
CSV_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'Hotel_Reviews.csv')
CSV_PATH = os.path.abspath(CSV_PATH)  # resolve to an absolute path so it works from any directory

KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'hotel-reviews'

# Load the data
df = pd.read_csv(CSV_PATH)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

for i, row in df.iterrows():
    if i >= 1000:  # cap at 1000 messages for testing (adjust or remove)
        break
    message = row.to_dict()  # all columns
    producer.send(TOPIC_NAME, value=message)
    print(f"Sent: {message}")
    time.sleep(0.1)  # throttle to simulate a live stream (can be removed)

producer.flush()
producer.close()
print("Done sending all reviews!")
