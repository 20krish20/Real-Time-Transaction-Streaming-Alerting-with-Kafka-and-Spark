# Real-Time Transaction Streaming Alerting with Kafka and Spark

This project implements a comprehensive real-time data pipeline designed to ingest, process, and analyze financial transaction streams to identify suspicious activity. It leverages **Apache Kafka** for high-throughput messaging and **Spark Structured Streaming** for low-latency, stateful data processing.

The pipeline processes simulated credit card transactions, enriches the data, applies fraud detection logic (e.g., spending velocity checks), and publishes alerts back to a dedicated Kafka topic and a low-latency serving layer (Snowflake/Delta Lake).

## 💡 Key Features

* **Real-Time Ingestion:** Uses a Python producer to simulate and push transactions to a Kafka topic.
* **Structured Streaming:** Spark Structured Streaming provides fault-tolerant, exactly-once processing.
* **Multi-Stage Processing:** Implements a Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold) for data quality and transformation.
* **Fraud Alerting:** Suspicious transactions are identified and immediately pushed to a separate Kafka alert topic.
* **Cloud-Native Design:** Deployable via Docker/Kubernetes, making it scalable and robust.

## 🧱 Architecture Overview

The system follows a standard Lambda/Kappa architecture pattern using stream-processing components:

1.  **Producer:** Python script (`producer/`) simulates card transactions and sends JSON messages to the input Kafka topic (`transactions`).
2.  **Kafka Broker:** Acts as the central message queue for ingestion and alert notification.
3.  **Spark Structured Streaming:** Reads the input Kafka topic, performs the following pipeline stages:
    * **Bronze:** Raw JSON ingestion, schema enforcement, and timestamp addition.
    * **Silver:** Data enrichment, JSON parsing, and basic cleaning.
    * **Gold (Alerting):** Applies business logic (e.g., windowed aggregates/velocity checks) to detect anomalies.
4.  **Serving Layer:** Processed data and final alerts are written to persistent storage (e.g., Delta Lake/Snowflake) for consumption by dashboards or downstream systems.

## 🛠️ Project Setup and Requirements

### Prerequisites

* **Python 3.x**
* **Git**
* **Docker & Docker Compose** (for running Kafka)
* **Apache Spark** (configured via environment, Databricks, or local cluster)

### Local Environment Setup

1.  **Clone the Repository:**
    ```bash
    git clone [https://github.com/20krish20/Real-Time-Transaction-Streaming-Alerting-with-Kafka-and-Spark.git](https://github.com/20krish20/Real-Time-Transaction-Streaming-Alerting-with-Kafka-and-Spark.git)
    cd Real-Time-Transaction-Streaming-Alerting-with-Kafka-and-Spark
    ```
2.  **Create and Activate Virtual Environment:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```
3.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## 🚀 Running the Pipeline

### Step 1: Start Kafka Broker

Use Docker Compose to launch a Kafka broker and a Zookeeper instance:

```bash
docker-compose -f infra/kafka-docker-compose.yml up -d
