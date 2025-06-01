# Mini Data Platform

This project implements a mini data platform using Docker containers that simulates a business process, ingests data into PostgreSQL, captures changes with Debezium, streams data through Kafka, processes it in Spark, and stores it in MinIO in Delta format.

## Objective

Develop a mini data platform using Docker containers that simulates a business process, ingests data into PostgreSQL, captures changes with Debezium, streams data through Kafka, processes it in Spark, and stores it in MinIO in Delta format.

## Table of Contents

- [Project Structure](#project-structure)
- [Requirements](#requirements)
- [Component Overview](#component-overview)
- [How to Deploy (from Scratch)](#how-to-deploy-from-scratch)
- [Monitoring & Logs](#monitoring--logs)
- [Development](#development)
- [License](#license)

## Project Structure

```bash
.
├── .env
├── docker-compose.yml
├── README.md
├── config/
│   └── debezium/
│       ├── connector_config.json
│       └── confluentinc-kafka-connect-avro-converter-7.9.0/
├── data/
│   └── input/
│       ├── customers.csv
│       ├── orders.csv
│       └── products.csv
├── logs/
├── services/
│   ├── data_generator/
│   ├── data_initializer/
│   ├── debezium_configurator/
│   ├── kafka_consumer/
│   └── spark_processor/
└── source/
    ├── data_generator/
    ├── data_initializer/
    ├── debezium_configurator/
    ├── kafka_consumer/
    ├── spark_processor/
    └── utilities/
```

## Requirements

- [Docker](https://www.docker.com/) (v20+ recommended)
- [Docker Compose](https://docs.docker.com/compose/) (v2+)
- 8GB+ RAM recommended for Spark and Kafka
- Unix-like OS (Linux/macOS recommended)
- Python 3.11+ (for local development, not needed for Docker deployment)

## Component Overview

- **PostgreSQL**: Main transactional database.
- **Data Initializer**: Loads initial data from CSVs into PostgreSQL.
- **Data Generator**: Simulates business events and inserts/updates data in PostgreSQL.
- **Debezium (Kafka Connect)**: Captures changes from PostgreSQL and streams them to Kafka topics.
- **Kafka**: Message broker for streaming data.
- **Schema Registry**: Manages Avro schemas for Kafka topics.
- **Kafka Consumer**: Reads and validates messages from Kafka topics.
- **Spark Processor**: Reads from Kafka, processes data, and writes to MinIO in Delta format.
- **MinIO**: S3-compatible object storage for processed data.
- **Zookeeper**: Coordinates Kafka brokers.

## Task-by-Task Breakdown & Deliverables

### Task 1: Simulating a Business Process with Python

- **Topics**: Python script to simulate business process, read CSVs, create tables in PostgreSQL, insert data.
- **Deliverables**:
  - Python script that reads three CSV files, creates PostgreSQL tables dynamically, and inserts data.
  - Updated `docker-compose.yml` with PostgreSQL service.

### Task 2: Connecting Debezium to Capture Changes in PostgreSQL

- **Topics**: Debezium setup, Kafka connector, AVRO/JSON serialization.
- **Deliverables**:
  - Debezium service in `docker-compose.yml`.
  - PostgreSQL with logical replication enabled.
  - Debezium configured for AVRO messages.
  - Verified change capture from PostgreSQL to Kafka in AVRO format.

### Task 3: Kafka Setup & Streaming Events in AVRO Format

- **Topics**: Kafka cluster, AVRO topics, Schema Registry, Kafka consumer.
- **Deliverables**:
  - Kafka cluster in `docker-compose.yml`.
  - Kafka with AVRO serialization.
  - Schema Registry for AVRO schemas.
  - Kafka consumer script to read and decode AVRO messages.

### Task 4: Integrating Spark with Kafka for Data Processing

- **Topics**: Spark Structured Streaming, Kafka integration, AVRO deserialization, basic transformations.
- **Deliverables**:
  - Spark container in `docker-compose.yml`.
  - Spark job reading AVRO messages from Kafka.
  - Basic transformation on streamed data.

### Task 5: Storing Processed Data in MinIO using Delta Lake

- **Topics**: MinIO as S3-compatible store, Spark DataFrames in Delta format, MinIO configuration.
- **Deliverables**:
  - MinIO in `docker-compose.yml`.
  - Spark job writing processed data to MinIO in Delta format.
  - Documentation on retrieving data from MinIO.

### Task 6: Automating Deployment & Ensuring Reliability

- **Topics**: Automated deployment, Docker volumes/networks, failure handling, data consistency.
- **Deliverables**:
  - Fully automated `docker-compose.yml` with all services.
  - Updated documentation for scratch deployment.

## How to Deploy (from Scratch)

### 1. Clone the Repository

```sh
git clone <https://github.com/mchojna/mini-data-platform>
cd mini-data-platform
```

### 2. Configure Environment

- Edit `.env` to adjust ports, credentials, or resource limits if needed.

### 3. Build and Start the Platform

```sh
docker compose build
docker compose up -d
```

This will:

- Start all services in the correct order.
- Initialize the database and load initial data.
- Configure Debezium and Kafka Connect.
- Start data generation, streaming, and processing.

### 4. Check Service Status

```sh
docker compose ps
```

### 5. View Logs

```sh
docker compose logs -f
```

### 6. Access MinIO Console

- Open [http://localhost:9001](http://localhost:9001) (default credentials: `minioadmin` / `minioadmin`).

## Monitoring & Logs

- **Centralized logs** are stored in the `logs/` directory (mounted from containers).
- **Service health** is monitored via Docker Compose healthchecks.
- **Spark UI**: [http://localhost:8080](http://localhost:8080)
- **MinIO Console**: [http://localhost:9001](http://localhost:9001)
- **Kafka Connect UI**: [http://localhost:8083](http://localhost:8083)
- **Schema Registry UI**: [http://localhost:8081](http://localhost:8081)

## Development

### Local Development

- All Python source code is in [`source/`](source/) and shared utilities in [`source/utilities/`](source/utilities/).
- Each service has its own `requirements.txt` for dependencies.
- To run a service locally, install dependencies and set up a `.env` file as needed.

Example (for data generator):

```sh
cd source/data_generator
pip install -r requirements.txt -r ../utilities/requirements.txt
python data_generator.py
```

### Adding New Services

- Add your code in `source/<service_name>/`.
- Create a Dockerfile in `services/<service_name>/`.
- Update `docker-compose.yml` to add the new service.

### Testing

- Unit tests should be placed in `source/<service_name>/tests/`.
- Use `pytest` or `unittest` as appropriate.


## License

This project is for educational and demonstration purposes. See individual component licenses for details.
