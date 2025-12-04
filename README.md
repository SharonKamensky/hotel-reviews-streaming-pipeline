# Hotel Reviews Real-Time Streaming Pipeline

A full real-time big-data pipeline for collecting, processing, analyzing, and visualizing hotel reviews using **Kafka**, **Spark Streaming**, **Elasticsearch**, and **Kibana**.

---

## 🎯 Business Motivation

Hotels and travel companies receive **massive volumes of online customer reviews** every day.  
These reviews contain valuable insights about:

- Customer satisfaction  
- Service quality  
- Hotel performance  
- Trending issues or complaints  
- Geographic behavior differences  

However, companies typically struggle to operationalize this data because:

- Reviews arrive continuously and require **real-time processing**
- Manual analysis is slow and unscalable
- Unstructured text data is hard to analyze without automation
- Trends and anomalies are often detected too late

This project demonstrates how a modern data engineering pipeline converts raw reviews into **real-time actionable insights**, enabling:

### 💼 Business Value
- Early detection of satisfaction drops  
- Identifying operational issues in hotels  
- Monitoring geographic reviewer behavior  
- Extracting trending keywords (positive & negative)  
- Supporting data-driven strategic decisions  
- Reducing manual analysis time from hours → seconds  

This real-time solution reflects how modern hospitality companies improve customer experience at scale.

---

## 📌 Project Overview

This project implements an end-to-end streaming pipeline for:

- Ingesting 500K+ hotel reviews  
- Streaming text into Kafka topics  
- Processing data in real time using Spark Streaming  
- Cleaning & enriching each review  
- Indexing structured results into Elasticsearch  
- Displaying insights on live Kibana dashboards  

The goal is to simulate a **production-grade data engineering environment**.

---

## 🧱 System Architecture

```
+------------+        +-----------+        +-----------------+        +-----------------+        +---------+
|  Producer  | -----> |  Kafka    | -----> | Spark Streaming | -----> | Elasticsearch   | -----> | Kibana  |
+------------+        +-----------+        +-----------------+        +-----------------+        +---------+
        (Python + CSV input)           (Processes real-time reviews)   (Stores structured docs)   (Visual dashboards)
```

---

## 🔄 Flow Explanation (Step-by-Step)

1. **Producer (Python + Kafka)**  
   - Reads raw hotel reviews from a large CSV  
   - Sends each review into a Kafka topic `hotel-reviews`  
   - Simulates real-time data streaming  

2. **Kafka Broker**  
   - Buffers and distributes messages  
   - Fault-tolerant data delivery  

3. **Spark Streaming Engine**  
   - Consumes reviews in micro-batches  
   - Cleans text, removes noise  
   - Extracts structured fields  
   - Prepares documents for indexing  

4. **Elasticsearch**  
   - Stores structured JSON documents  
   - Enables full-text search & aggregations  

5. **Kibana Dashboard**  
   - Displays real-time analytics  
   - KPIs, trends, demographics, keyword frequency  
   - Top hotels by reviews, satisfaction trends, etc.

---

## 📁 Repository Structure

```
hotel-reviews-streaming-pipeline/
│
├── consumers/               # Kafka consumers
├── producers/               # Review stream producers
├── spark_app/               # Spark Streaming job
├── jars/                    # Kafka–Spark connector JARs
├── docs/                    # Documentation
│   └── Big_Data_Project.pdf # Full project PDF
│
├── docker-compose.yml       # Deploys Kafka, Zookeeper, ES, Kibana
├── README.md                # This file
└── LICENSE
```

---

## 📊 Dashboards (Kibana)

The system provides real-time visualization dashboards, including:

### ⭐ Full KPI Overview
- Total number of reviews  
- Average rating  
- Review volume over time  
- Satisfaction index  

### ⭐ Trends & Hotel Insights
- Top hotels by number of reviews  
- Review geographic distribution  
- Rating trends over time  

### ⭐ Keyword Intelligence
- Top positive keywords  
- Top negative keywords  
- Word-frequency analysis  

(Images can be added here once uploaded to the repo.)

---

## 🚀 How to Run the Project

### 1️⃣ Start the infrastructure
```
docker-compose up -d
```

This launches:
- Kafka  
- Zookeeper  
- Elasticsearch  
- Kibana  

### 2️⃣ Run the Producer
```
python producers/send_reviews_to_kafka.py
```

### 3️⃣ Run the Spark Streaming Job
```
python spark_app/spark_kafka_stream.py
```

### 4️⃣ Open Kibana Dashboard
Navigate to:
```
http://localhost:5601
```

---

## 🛠 Technologies Used

- **Kafka** — Real-time ingestion  
- **Zookeeper** — Kafka coordination  
- **Spark Streaming** — Real-time processing  
- **Elasticsearch** — Fast indexing & querying  
- **Kibana** — Dashboards & visualization  
- **Python** — Producers + consumer logic  

---

## 📌 Future Improvements

- Apply ML sentiment analysis  
- Add anomaly detection for sudden rating drops  
- Deploy with Kubernetes  
- Scale Spark cluster for higher throughput  

---

## 👤 Author

Sharon Kamensky — B.Sc. in Mathematics (Statistics & Data Science)  
Aspiring Data Analyst / Data Engineer  
GitHub: https://github.com/SharonKamensky


