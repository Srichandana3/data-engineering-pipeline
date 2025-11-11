# 🚀 Data Engineering End-to-End Pipeline

### 🎯 **Goal**
This project demonstrates a complete **real-time data engineering pipeline** that simulates financial transactions flowing through a modern data architecture.  
Data flows through **Kafka → MinIO (Raw Zone) → Spark (ETL) → MinIO (Processed Zone) → Postgres → Metabase** for analytics and visualization.

---

## 🧱 **Architecture Overview**

```
         ┌───────┐
         │  Kafka   │  ←─ Transaction Producer (Python)
         └──┌────┐
              │
              ▼
         ┌───────┐
         │  MinIO   │  ←─ Raw Zone Storage (JSON)
         └──┌────┐
              │
              ▼
         ┌───────┐
         │  Spark   │  ←─ Cleans & Transforms data → Writes to Processed Zone (Parquet)
         └──┌────┐
              │
              ▼
         ┌───────┐
         │ Postgres │  ←─ Stores curated data for BI consumption
         └──┌────┐
              │
              ▼
         ┌───────┐
         │ Metabase │  ←─ Visualizes transactions and insights
         └───────┘
```

---

## 🧩 **Technologies Used**

| Component | Purpose | Technology |
|------------|----------|-------------|
| **Data Ingestion** | Stream real-time transactions | 🔸 Apache Kafka |
| **Object Storage** | Store raw & processed files | 🟢 MinIO (S3-compatible) |
| **ETL Processing** | Clean, transform, and load data | 🔥 Apache Spark |
| **Database** | Store curated data | 🐘 PostgreSQL |
| **Visualization** | Build dashboards | 📊 Metabase |
| **Containerization** | Run all services together | 🐳 Docker & Docker Compose |

---

## ⚙️ **Setup & Run**

### 1️⃣ Start All Services
```bash
docker-compose up -d
```

✅ This spins up:
- Zookeeper  
- Kafka  
- Spark Master & Worker  
- MinIO  
- PostgreSQL  
- Metabase  

Check:
```bash
docker ps
```

---

### 2️⃣ Produce Transactions to Kafka
Run the Python producer to send fake transactions:
```bash
python scripts/producer_to_kafka.py
```

---

### 3️⃣ Consume from Kafka → MinIO (Raw Zone)
This stores each Kafka message as a `.json` file in MinIO:
```bash
python scripts/consumer_to_minio.py
```

Check your **MinIO console** at:  
🔗 [http://localhost:9000](http://localhost:9000)  
(username: `minioadmin`, password: `minioadmin`)

---

### 4️⃣ Run ETL in Spark
Now process data from MinIO raw → curated:
```bash
docker exec -it spark-master bash -lc '/opt/spark/bin/spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  /opt/spark-app/etl_spark_to_processed.py --endpoint http://minio:9000'
```

This creates **clean Parquet files** in the `processed` bucket and writes curated data into **PostgreSQL**.

---

### 5️⃣ Visualize in Metabase
- Open: [http://localhost:3000](http://localhost:3000)  
- Connect the `analytics` database (Postgres)  
- Explore tables and create dashboards.

---

## 📂 **Project Structure**

```
data-engineering-project/
│
├── docker/
│   ├── docker-compose.yml
│   └── configs/
│
├── scripts/
│   ├── producer_to_kafka.py
│   ├── consumer_to_minio.py
│   └── etl_spark_to_processed.py
│
├── spark-app/
│   └── etl_spark_to_processed.py
│
└── README.md
```

---

## 🧠 **Key Learnings**
- End-to-end orchestration of **data streaming → storage → ETL → analytics**.
- Real-time ingestion with **Kafka** and object-based persistence using **MinIO**.
- Distributed ETL with **PySpark**.
- Post-ETL analytics integration via **PostgreSQL + Metabase**.
- Full containerized environment using **Docker Compose**.

---

## 🧩 **Future Enhancements**
- Integrate **Airflow** for scheduling ETL runs.  
- Add **data quality validation** with Great Expectations.  
- Deploy on **AWS (MSK + S3 + EMR + RDS)** for production-scale testing.

---

## 🛠️ **Tech Stack Badges**

![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-C72E49?style=for-the-badge&logo=minio&logoColor=white)
![Metabase](https://img.shields.io/badge/Metabase-509EE3?style=for-the-badge&logo=metabase&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

