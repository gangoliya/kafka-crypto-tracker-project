# Kafka Crypto Tracker

A real-time cryptocurrency market data pipeline built with Python, Apache Kafka (KRaft), and Docker. This project demonstrates a production-grade streaming architecture, ingesting live trade data from the Binance WebSocket API and processing it through a distributed Kafka cluster.

## 🏗️ Architecture

1.  **Data Source:** Binance WebSocket API (Real-time `btcusdt@trade` or `@ticker` streams).
2.  **Producer:** A Python-based Kafka Producer that ingests WebSocket events and pushes them to a Kafka topic.
3.  **Broker:** A 3-node Apache Kafka cluster running in KRaft mode (no Zookeeper required).
4.  **Monitoring:** Redpanda Console for a web-based UI to visualize topics and messages.
5.  **Consumer:** (In Progress) Python-based consumers to process and analyze the stream.

## 🛠️ Tech Stack

*   **Language:** Python 3.12+ (managed by `uv`)
*   **Streaming:** Apache Kafka (Dockerized)
*   **Libraries:** `confluent-kafka`, `pydantic-settings`, `websockets`
*   **UI:** Redpanda Console
*   **Environment:** Docker Compose

## 🚀 Getting Started

### 1. Prerequisites
*   [Docker & Docker Compose](https://docs.docker.com/get-docker/)
*   [uv](https://github.com/astral-sh/uv) (Python package manager)

### 2. Infrastructure Setup
Spin up the 3-node Kafka cluster and the Redpanda Console:
```bash
docker-compose up -d
```
Access the Redpanda Console at: [http://localhost:7070](http://localhost:7070)

### 3. Python Environment Setup
Install dependencies and create a virtual environment:
```bash
uv sync
```

### 4. Configuration
Create a `.env` file in the root directory (see `.env.example` if available):
```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_NAME=market_events
KAFKA_CLUSTER_ID=<Your Cluster ID here>
```

## 📖 Usage

### Create the Kafka Topic
Before producing data, ensure the topic is created with the correct partitions and replication:
```bash
uv run kafka_ui/scripts/create_topics.py
```

### Run the Market Data Producer
Start ingesting real-time Binance data into Kafka:
```bash
uv run kafka_ui/src/kafka_pipeline/producer/market_data_source.py
```

## 📂 Project Structure

*   `kafka_ui/src/kafka_pipeline/`: Core package.
    *   `producer/`: Kafka producer logic and data sources.
    *   `config/`: Pydantic-based settings management.
    *   `consumer/`: (Future) Stream processing and consumption logic.
*   `kafka_ui/scripts/`: Administrative scripts (topic creation, etc.).
*   `docker-compose.yml`: Defines the 3-node Kafka cluster.

## 🚧 Roadmap
- [x] 3-node Kafka Cluster setup.
- [x] Basic Producer with Binance integration.
- [x] Centralized Configuration (Pydantic Settings).
- [ ] Implement Pydantic Schemas for data validation.
- [ ] Implement Base Consumer.
- [ ] Add real-time price statistics processor.
- [ ] Integration testing with `testcontainers`.
