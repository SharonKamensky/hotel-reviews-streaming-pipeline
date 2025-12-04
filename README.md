# Hotel Reviews Real-Time Streaming Pipeline  
A full real-time big-data pipeline for collecting, processing, analyzing, and visualizing hotel reviews using **Kafka**, **Spark Streaming**, **Elasticsearch**, and **Kibana**.

---

## 📌 Project Overview
This project implements an end-to-end real-time data pipeline designed to ingest hotel reviews, process them using streaming analytics, enrich and prepare the data, and store it for real-time querying and dashboard visualization.

The pipeline simulates a real production environment and demonstrates hands-on experience with distributed systems, stream processing, data engineering workflows, and monitoring tools.

---

## 🏗️ System Architecture

```
+----------------+        +-------------+        +-----------------+
|   Producers    | -----> |    Kafka    | -----> | Spark Streaming |
+----------------+        +-------------+        +-----------------+
                                                            |
                                                            v
                                               +---------------------+
                                               |   Elasticsearch     |
                                               +---------------------+
                                                            |
                                                            v
                                               +---------------------+
                                               |      Kibana         |
                                               +---------------------+
```

### **Flow Explanation:**
1. **Producers** send raw hotel reviews into Kafka topics.  
2. **Kafka** acts as a distributed message broker that buffers and stores events.  
3. **Spark Streaming** consumes messages in micro-batches, processes, filters, and enriches reviews.  
4. The processed data is indexed into **Elasticsearch**.  
5. **Kibana** visualizes the data in real time using interactive dashboards.

---

## 📁 Repository Structure

```
hotel-reviews-streaming-pipeline/
│
├── consumers/               # Kafka consumers
├── producers/               # Kafka producers (hotel reviews simulation)
├── spark_app/               # Spark Streaming application
├── jars/                    # Required Spark–Kafka connector JARs
├── docs/                    # Documentation (including full project PDF)
│     └── Big_Data_Project.pdf
│
├── docker-compose.yml       # Launches Kafka, Zookeeper, Elasticsearch & Kibana
├── README.md                # This file
└── LICENSE
```

---

## 🔧 Technologies Used

### **Messaging & Streaming**
- Apache Kafka  
- Zookeeper  

### **Processing**
- Apache Spark Streaming  
- PySpark  

### **Storage**
- Elasticsearch  

### **Visualization**
- Kibana  

### **Orchestration**
- Docker & Docker Compose  

### **Programming**
- Python  

---

## ⚙️ How to Run the Project

### 1️⃣ Clone the repository

```bash
git clone https://github.com/SharonKamensky/hotel-reviews-streaming-pipeline.git
cd hotel-reviews-streaming-pipeline
```

---

### 2️⃣ Start the infrastructure (Kafka, Zookeeper, Elasticsearch, Kibana)

```bash
docker-compose up
```

This will automatically launch:
- Kafka  
- Zookeeper  
- Elasticsearch  
- Kibana (http://localhost:5601)

---

### 3️⃣ Run Producers (hotel reviews stream)

Inside the `producers/` directory:

```bash
python send_all_columns_to_kafka.py
```

---

### 4️⃣ (Optional) Run Consumers

Inside the `consumers/` directory:

```bash
python print_reviews_consumer.py
```

---

### 5️⃣ Run Spark Streaming App

Inside `spark_app/`:

```bash
python spark_kafka_dashboard.py
```

Spark will:
- Consume messages from Kafka  
- Process, clean, filter & enrich reviews  
- Index results into Elasticsearch  

---

### 6️⃣ View Real-Time Visualization (Kibana)

📍 Open:  
**http://localhost:5601**

Create an index pattern for:

```
reviews_index
```

Explore:
- Review sentiment  
- Keyword frequency  
- Review volume over time  
- Reviewer patterns  
- Any custom Kibana dashboards you create  

---

## ⭐ Key Features

- Real-time ingestion pipeline  
- Scalable Kafka messaging  
- Distributed computation via Spark Streaming  
- Elasticsearch search & analytics engine  
- Beautiful dashboards in Kibana  
- Fully containerized with Docker  
- Clean modular Python structure  

---

## 📄 Full Project Documentation

A full explanation + presentation is available here:

👉 `docs/Big_Data_Project.pdf`

---

## 🚀 Future Improvements

- Add NLP-based sentiment analysis (VADER, BERT)  
- Add anomaly detection for suspicious reviews  
- Move from Spark Streaming → Structured Streaming  
- Integrate Schema Registry (Confluent)  
- Deploy pipeline on Kubernetes  
- Add API layer for application consumption  

---

## 👩‍💻 Author

**Sharon Kamensky**  
B.Sc. in Statistics & Data Science  
Aspiring Data Analyst / Data Engineer  
Passionate about building scalable, modern data solutions.

---
