# Real-Time Stock Market Analysis Pipeline

A containerised, end-to-end streaming data pipeline that pulls intraday stock prices from an API, streams them through **Apache Kafka**, processes them in real time with **Apache Spark Structured Streaming**, and stores them in **PostgreSQL** for dashboarding and analysis. It was built as a hands-on project for **MarketPulse Analytics**, a (fictional) New York-based financial analytics firm.

---

## Table of Contents

1. [Project Introduction](#1-project-introduction)
2. [Business Problem](#2-business-problem)
3. [Key Features](#3-key-features)
4. [Tools Used](#4-tools-used)
5. [Architecture](#5-architecture)
6. [Project Structure](#6-project-structure)
7. [Data Model](#7-data-model)
8. [How to Reproduce This Project](#8-how-to-reproduce-this-project)
9. [Future Improvements](#9-future-improvements)
10. [Author](#10-author)

---

## 1. Project Introduction

**MarketPulse Analytics** (founded 2016, New York City) provides real-time stock market analytics to investment managers, hedge funds and private equity firms. Its value rests on one promise: *deliver accurate market insight fast enough to act on.*

This project builds the real-time data backbone behind that promise. It:

- **Ingests** 5-minute intraday prices for **TSLA, MSFT and GOOGL** from the Alpha Vantage API (via RapidAPI).
- **Streams** each price record into a Kafka topic (`stock_analysis`).
- **Processes** the stream with a PySpark Structured Streaming job running on a Spark cluster.
- **Persists** the cleaned records into a PostgreSQL table (`stocks`) that BI tools can query.
- Runs the whole infrastructure in **Docker**, so it can be started with a single command.

---

## 2. Business Problem

Financial markets move in seconds, and MarketPulse's clients increasingly expect insight at the same speed. The company's existing infrastructure struggled to keep up, particularly during volatile trading periods:

| Pain point | Operational impact |
|---|---|
| **High latency** | Delays in processing market data made reports less accurate and less timely, which hurt client decision-making. |
| **Increased risk** | Traders acting on stale information are more likely to make poor investment decisions. |
| **Poor scalability** | The system did not scale to large datasets at peak market hours, delaying critical insights. |
| **Limited monitoring** | Without robust system monitoring, bottlenecks and failures were found late. |

**Why solve it?** Faster, more reliable data gives MarketPulse a first-mover advantage over competitors, improves operational efficiency, increases client satisfaction and retention, and creates a platform that can scale with market data volumes.

### Project objectives

1. **Scalable data pipeline**: a fault-tolerant, streaming pipeline built on Apache Kafka.
2. **Real-time reporting**: data modelled and stored so dashboards (Power BI) can visualise market trends.
3. **Low latency & data accuracy**: processing and delivering market data in near real time.
4. **System monitoring**: visibility into the health of each pipeline component.

---

## 3. Key Features

- **Real-time ingestion**: a Python producer calls the Alpha Vantage `TIME_SERIES_INTRADAY` endpoint (5-minute interval) for multiple tickers and publishes every record to Kafka as JSON.
- **Decoupled streaming architecture**: Kafka sits between ingestion and processing, so each side can fail, restart or scale independently.
- **Kafka in KRaft mode**: runs without ZooKeeper, with separate internal (`9092`) and external (`9094`) listeners for containers and host clients.
- **Spark Structured Streaming consumer**: parses JSON against an explicit schema, casts timestamps, and writes micro-batches to PostgreSQL through `foreachBatch` + JDBC.
- **Fault tolerance**: Spark checkpointing is enabled, and the consumer container is set to `restart: unless-stopped`.
- **Fully containerised**: a single `compose.yml` defines Kafka, Spark (master + worker), the consumer job, PostgreSQL, Kafka UI and pgAdmin.
- **Built-in observability**: Kafka UI (topics, messages, consumer lag), Spark Master UI (jobs and workers), pgAdmin (data inspection) and file-based application logging in the producer.
- **Secure configuration**: API key and database credentials are loaded from a git-ignored `.env` file, not hard-coded in the Python code.
- **Resilient API handling**: request errors (e.g. `401`, `429`) are caught and logged instead of crashing the producer.

---

## 4. Tools Used

| Layer | Tool | Role in the project |
|---|---|---|
| **Language** | Python 3 | Producer, API extraction and the PySpark job |
| **Data source** | Alpha Vantage API (via RapidAPI) | Intraday stock prices |
| **Streaming backbone** | Apache Kafka 7.4 (Confluent, KRaft mode) | Message broker for the real-time pipeline |
| **Stream processing** | Apache Spark 3.5.1 (Structured Streaming / PySpark) | Reads from Kafka, transforms and loads data |
| **Storage** | PostgreSQL 17 (`debezium/postgres`) | Stores the processed stock data |
| **Containerisation** | Docker & Docker Compose | Runs and connects all services |
| **Monitoring / admin** | Kafka UI, Spark Master UI, pgAdmin 4 | Inspect topics, jobs and database |
| **Reporting** | Power BI | Interactive dashboards on top of PostgreSQL |
| **Python libraries** | `kafka-python`, `requests`, `python-dotenv` | Kafka client, HTTP calls, environment config |
| **Version control** | Git / GitHub | Source control |

---

## 5. Architecture

![Architecture Diagram](docs/architecture.mov)

*Figure 1: End-to-end architecture of the real-time stock analysis pipeline.*

**Data flow (for reference when drawing the diagram):**

```
Alpha Vantage API (RapidAPI)
        │  HTTP GET (5-min intraday, TSLA / MSFT / GOOGL)
        ▼
Python Producer  ──►  Kafka topic: stock_analysis   (localhost:9094 from host, kafka:9092 inside Docker)
                                  │
                                  ▼
              Spark Structured Streaming job (spark-master + spark-worker)
                   parse JSON ► apply schema ► cast timestamp ► micro-batch write
                                  │
                                  ▼
                    PostgreSQL  (database: stock_data, table: stocks)
                                  │
                                  ▼
                       Power BI dashboard / pgAdmin
```

---

## 6. Project Structure

```
realtime_insights/
├── compose.yml                  # All infrastructure services
├── .env                         # Secrets
├── .gitignore
├── producer/                    # Runs on the host machine
│   ├── config.py                # Logging setup, API URL, request headers
│   ├── extract.py               # Calls the API and flattens the JSON response
│   ├── producer_setup.py        # Kafka producer config + topic name
│   └── main.py                  # Extract and publish to Kafka
├── consumer/                    # Runs inside Docker 
│   ├── Dockerfile               # Spark image + spark-submit command
│   ├── config.py                # Postgres config, checkpoint dir, JSON schema
│   └── consumer.py              # Structured Streaming job from Kafka to PostgreSQL
└── docs/
    └── architecture.mov         # Architecture Diagram
```

---

## 7. Data Model

Each Kafka message is a JSON object representing one 5-minute price bar:

```json
{
  "date":   "2026-10-27 15:55:00",
  "symbol": "TSLA",
  "open":   "250.10",
  "high":   "251.30",
  "low":    "249.80",
  "close":  "250.95"
}
```

The Spark job writes these to the **`stocks`** table in the **`stock_data`** database. The `date` field is cast to a `TIMESTAMP`; the price fields are currently passed through as strings (see [Future Improvements](#11-future-improvements)).

| Column | Source field | Description |
|---|---|---|
| `date` | `Time Series` key | Timestamp of the 5-minute bar |
| `symbol` | `Meta Data → 2. Symbol` | Ticker (TSLA, MSFT, GOOGL) |
| `open` | `1. open` | Opening price |
| `high` | `2. high` | Highest price in the interval |
| `low` | `3. low` | Lowest price in the interval |
| `close` | `4. close` | Closing price |

---

## 8. How to Reproduce This Project

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose (v2)
- Python 3.10+ and `pip`
- Git
- A free [RapidAPI](https://rapidapi.com/) account subscribed to the **Alpha Vantage** API (this gives you an API key)
- ~4 GB of free RAM for the containers

### Step 1: Clone the repository

```bash
git clone https://github.com/adetanchelsea/realtimestockinsights.git
cd realtimestockinsights
```

### Step 2: Create the `.env` file

Create a file named `.env` in the project root:

```env
API_KEY="your_rapidapi_key_here"
POSTGRES_USER="admin"
POSTGRES_PASSWORD="admin"
```

### Step 3: Start the infrastructure

```bash
docker compose up -d --build
```

This starts Kafka, Spark master and worker, the Spark streaming consumer, PostgreSQL, Kafka UI and pgAdmin. The first start takes a few minutes because Spark downloads the Kafka and PostgreSQL connector packages.

Confirm everything is up:

```bash
docker compose ps
docker logs -f consumer    
```

### Step 4: Set up the producer environment

In a **new terminal**, from the project root:

```bash
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install kafka-python requests python-dotenv
```

### Step 5: Run the producer

> **Start the consumer first.** The Spark job reads with `startingOffsets = latest`, so it only picks up messages published *after* it starts.

```bash
cd producer
python main.py
```

The producer fetches data for TSLA, MSFT and GOOGL, then publishes each record to the `stock_analysis` topic, pausing 2 seconds between messages to simulate a live feed. Progress is printed to the console and logged to `stock_data_logs.log`.

### Step 6: Verify the pipeline end to end

**1. Kafka**: open Kafka UI → *Topics* → `stock_analysis` → *Messages* and confirm records are arriving.

**2. Spark**: open the Spark Master UI and confirm the `KafkaSparkStreaming` application is running.

**3. PostgreSQL**: query the table:

```bash
docker exec -it postgres_db psql -U admin -d stock_data \
  -c "SELECT symbol, COUNT(*) AS bars, MAX(date) AS latest FROM stocks GROUP BY symbol;"
```

You should see rows for `TSLA`, `MSFT` and `GOOGL`.

### Step 7: Connect to Power BI

Connect Power BI to PostgreSQL using:

- **Server:** `localhost:5434`
- **Database:** `stock_data`
- **Table:** `stocks`
- **User / Password:** as set in `.env`

If you use pgAdmin instead, register a server with host `postgres`, port `5432` (the container-network address).

### Shutting down

```bash
docker compose down        # stop containers
docker compose down -v     # stop containers and delete all data
```

---

## 9. Future Improvements

**Data & pipeline**
- Replace string price fields with proper numeric types (`DECIMAL`/`DOUBLE`) and cast them in the Spark job.
- Make writes **idempotent** (primary key on `symbol + date` with upsert) so re-running the producer doesn't create duplicates.
- Add **data-quality checks** (null/negative prices, schema validation, dead-letter topic for bad records).
- Add Spark **windowed aggregations** (moving averages, VWAP, volatility, % change) for richer real-time insights.
- Make the ticker list configurable (env var or config file) and support more exchanges.
- Move from polling a REST API to a true **WebSocket / market-data feed** for lower latency.
- Add **retry with exponential backoff** and rate-limit handling to the producer instead of stopping on the first error.

**Scalability & reliability**
- Run multiple Kafka brokers with replication and partition topics by symbol.
- Add more Spark workers and tune executors for peak trading hours.
- Store Spark checkpoints on a persistent volume (currently `/tmp` inside the container).

**Monitoring**
- Add **Prometheus + Grafana** for pipeline metrics (consumer lag, throughput, batch duration) and alerting.
- Replace file logging with structured, centralised logging.

**Engineering practices**
- Add a `requirements.txt` for the producer and pin dependency versions.
- Schedule producer runs with **Airflow** or cron; add unit tests and a CI pipeline (GitHub Actions).
- Add `stock_data_logs.log` to `.gitignore`.
- Move compose credentials into the `.env` file instead of hard-coding them.
- Fix the PostgreSQL volume mount path in `compose.yml` (`/var/lib/postgresql/data`) so data persists across container removal.
- Copy `config.py` into the consumer image (currently available only through the bind mount).

**Cloud & analytics**
- Deploy on AWS/Azure/GCP using managed Kafka and managed PostgreSQL.
- Add a time-series store (TimescaleDB) or a data warehouse for historical analysis.
- Add anomaly detection or ML-based price-movement signals.

---

## 10. Author

**Chelsea Adetan**
GitHub: [@adetanchelsea](https://github.com/adetanchelsea) · LinkedIn: [LinkedIn](https://www.linkedin.com/in/chelsea-adetan/) · Email: adetanchelsea@gmail.com

