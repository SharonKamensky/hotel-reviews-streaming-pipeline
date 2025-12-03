import pandas as pd
from kafka import KafkaProducer
import json
import time
import os

# נתיב יחסי לתוך data/
CSV_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'Hotel_Reviews.csv')
CSV_PATH = os.path.abspath(CSV_PATH)  # שיהיה 100% מדויק מכל מקום

KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'hotel-reviews'

# קריאת הדאטה
df = pd.read_csv(CSV_PATH)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

for i, row in df.iterrows():
    if i >= 1000:  # להגביל ל-1000 הודעות לבדיקה (אפשר לשנות/להוריד)
        break
    message = row.to_dict()  # כל העמודות!
    producer.send(TOPIC_NAME, value=message)
    print(f"Sent: {message}")
    time.sleep(0.1)  # רק לשם הדגמה, אפשר להוריד

producer.flush()
producer.close()
print("Done sending all reviews!")
