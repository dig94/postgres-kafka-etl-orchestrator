# Postgres–Kafka ETL Orchestrator

A modular data ingestion and orchestration framework built to automate table-level extraction from PostgreSQL using Kafka event triggers. This project demonstrates end-to-end data pipeline engineering using custom connectors, message-driven extraction, and production-ready architecture.

---

## 🚀 Project Overview

**automatically discover all tables in a source database and schedule extraction jobs using Kafka.**

- 🔌 **Pluggable connector architecture** (PostgreSQL, MySQL, and future sources)  
- 📡 **Kafka-based extraction triggers** per table  
- 🧩 **Extensible registry to load connectors dynamically**  
- 🗃️ **Schema-aware metadata handling**  
- 🐳 **Docker support for local orchestration**  
- ⚙️ **Airflow-ready pipeline layout for future scheduling**  
- 🧱 Starter modules for loader, extractor, Spark, and MinIO

This reflects my hands-on experience building real ETL systems using Python, Kafka, and database connectors.

## 📁 Project Structure
Project-1-version2/
│
├── connectors/
│ ├── base_connector.py # Abstract connector logic
│ ├── postgres.py # PostgreSQL JDBC-style connector
│ ├── my_sql.py # MySQL connector (extensible)
│ ├── registry.py # Dynamically loads connectors by name
│ └── config.py # Central configuration
│
├── extractor/
│ ├── kafka_producer.py # Wrapper over confluent_kafka Producer
│ └── extractor_service.py # Sends extraction messages using Kafka
│
├── loader/ # Future: sink layer (S3, MinIO, DB)
├── spark/ # Future: Spark ingestion jobs
├── airflow/ # Future: Airflow DAG integration
│
├── main.py # Entrypoint: enumerate tables & publish Kafka events
├── docker-compose.yaml # Kafka, Zookeeper, DB, Airflow (optional)
└── requirements.txt


## 🧠 How It Works

### **1. Connector Auto-Discovery**
`PostgresConnector()` connects to the database and lists all tables via metadata queries.

### **2. Event Creation**
For each table, the system creates a standardized Kafka event:


"source": "postgres",
"table": "<table_name>",
"action": "extract",
"timestamp": "<ts>"
}


### **3. Kafka Publishing**
`KafkaProducerClient` publishes messages to the configured topic (e.g., `extract.trigger`).

### **4. Downstream Consumers**
Future modules (Spark, loaders, S3/MinIO writers) consume these events to run:
- Data extraction
- Transformations
- Loads to data lake or warehouse

This pattern modernizes the ETL workflow into **event-driven micro-pipelines**.

---

## 🏗️ Technologies Used

- **Python 3.11**
- **Kafka (Confluent / Open Source)**
- **PostgreSQL**
- **Docker & Docker Compose**
- **Airflow-ready structure**
- **Spark JARs (for future expansion)**

---

## 🎯 What This Project Demonstrates About My Experience

- Building **custom data connectors** (Postgres, MySQL)
- Designing **decoupled ETL architectures**
- Implementing **event-driven ingestion pipelines using Kafka**
- Using **metadata-driven table discovery**
- Structuring production-style Python services
- Working with **Dockerized orchestration**, Airflow layouts, and Spark
- Ability to design and build **end-to-end data engineering systems**

This project highlights the foundational workflows used in real enterprise data platforms.

---

## 🚦 How to Run

1. Start dependent services
2. Run connector
3. Check Kafka messages

## 🛠️ Future Enhancements

- Add CDC using Debezium  
- Implement MinIO ingestion layer  
- Add Spark-based transformation jobs  
- Build Airflow DAGs for full orchestration  
- Implement dynamic schema evolution 
