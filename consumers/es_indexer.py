"""
Kafka -> Elasticsearch indexer.

Consumes reviews from the `hotel-reviews` topic, cleans them, and indexes
them into Elasticsearch so Kibana can visualize them live.

Note: rebuilt from the original project design (the original script was not
preserved). Same logic as described in docs/Big_Data_Project.pdf.
"""
import hashlib
import json
import math
from datetime import datetime

from elasticsearch import Elasticsearch, helpers
from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
TOPIC = "hotel-reviews"
ES_URL = "http://localhost:9200"
INDEX = "hotel-reviews"
BATCH_SIZE = 500

NUMERIC_FIELDS = ["Reviewer_Score", "Average_Score", "lat", "lng",
                  "Review_Total_Negative_Word_Counts", "Review_Total_Positive_Word_Counts"]


def review_id(r):
    """Deterministic ID -> re-running the pipeline overwrites instead of duplicating."""
    key = f"{r.get('Hotel_Name')}|{r.get('Review_Date')}|{r.get('Positive_Review')}|{r.get('Negative_Review')}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def clean(r):
    doc = {}
    for k, v in r.items():
        if v is None or (isinstance(v, float) and math.isnan(v)):
            continue                                   # drop missing values
        doc[k] = v.strip() if isinstance(v, str) else v
    for f in NUMERIC_FIELDS:                           # cast numbers for aggregations
        if f in doc:
            try:
                doc[f] = float(doc[f])
            except (TypeError, ValueError):
                doc.pop(f)
    if "Review_Date" in doc:                           # ISO date so Kibana's time filter works
        try:
            doc["Review_Date"] = datetime.strptime(doc["Review_Date"], "%m/%d/%Y").date().isoformat()
        except ValueError:
            doc.pop("Review_Date")
    for f in ("Positive_Review", "Negative_Review"):   # dataset placeholders -> empty
        if doc.get(f) in ("No Positive", "No Negative"):
            doc[f] = ""
    doc["Review_ID"] = review_id(r)
    return doc


def main():
    es = Elasticsearch(ES_URL)
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="es-indexer",
    )
    total = 0
    print("Indexing reviews into Elasticsearch... (Ctrl+C to stop)")
    try:
        while True:
            # poll returns whatever arrived in the last second -> near-real-time bulk writes
            records = consumer.poll(timeout_ms=1000, max_records=BATCH_SIZE)
            actions = []
            for msgs in records.values():
                for msg in msgs:
                    doc = clean(msg.value)
                    actions.append({"_index": INDEX, "_id": doc["Review_ID"], "_source": doc})
            if actions:
                helpers.bulk(es, actions)
                total += len(actions)
                print(f"Indexed {total} reviews")
    except KeyboardInterrupt:
        pass
    finally:
        print(f"Stopped. Indexed {total} reviews.")
        consumer.close()


if __name__ == "__main__":
    main()
