from kafka import KafkaConsumer
import json

# התחברות ל-Kafka (localhost)
consumer = KafkaConsumer(
    'hotel-reviews',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',  # לקרוא גם הודעות ישנות מההתחלה
    enable_auto_commit=True
)

print("Waiting for messages... (press Ctrl+C to stop)")
try:
    for message in consumer:
        review = message.value
        print("=" * 50)
        print("New review:")
        for key, value in review.items():
            print(f"{key}: {value}")
except KeyboardInterrupt:
    print("\nStopped consumer.")

consumer.close()
