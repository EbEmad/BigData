# Big Data Projects 

This repository contains a collection of projects that demonstrate different aspects of **Big Data processing**, including **batch processing, streaming, and Hadoop/Hive-based analytics**.  
The goal is to showcase practical implementations of data engineering concepts using widely adopted technologies like **Apache Spark, Hadoop, and Hive**.

---

##  Projects Overview

### 1. `hadoop-hive-processing`

This project demonstrates **data storage and querying using Hadoop and Hive**.  

#### 🔹 Description
- Hive provides a SQL-like interface to manage and analyze data stored in Hadoop’s HDFS.  
- This project covers how to:
  - Load raw datasets into Hive tables
  - Run analytical queries at scale
  - Use HiveQL for structured data exploration  

#### 🔹 Tech Stack
- **Hadoop (HDFS)** for distributed data storage  
- **Hive** for SQL-like querying  
- **SQL** for data analytics  

#### 🔹 Usage
1. Start Hadoop and Hive services.  
2. Load sample data into Hive tables (scripts provided inside the project).  
3. Run Hive queries for data analysis.

---

### 2. `spark_batch_processing`

This project focuses on **batch ETL pipelines using Apache Spark**.  

#### 🔹 Description
- Batch processing is suitable for large datasets that don’t require real-time handling.  
- This project shows how to:
  - Read data from files (CSV, JSON, Parquet, etc.)  
  - Perform transformations (filtering, grouping, aggregations)  
  - Write output to storage systems or databases  

#### 🔹 Tech Stack
- **Apache Spark** (PySpark) for distributed batch processing  
- **Python** for pipeline implementation  

#### 🔹 Usage
Run a batch job with Spark:  
```bash
spark-submit batch_job.py 
```

### 3. `Spark Streaming`

This project demonstrates **real-time data processing with Apache Spark Streaming**.

---

## 🔹 Description
Processes data as it arrives, suitable for **monitoring, fraud detection, and dashboards**.  
Key tasks:  
- Ingest data (e.g., from **Kafka** or sockets)  
- Process & aggregate streams  
- Handle **windowed operations**  

---

## 🔹 Tech Stack
- Apache Spark Streaming  
- Kafka (optional)  
- Python (PySpark)  

---

## 🔹 Usage
Run a streaming job:  
```bash
spark-submit streaming_job.py
```

##  Setup

1. Clone repo & navigate:
   ```bash
   git clone https://github.com/EbEmad/BigData.git
   cd BigData
```


