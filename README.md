# Real-Time Cryptocurrency Analytics Pipeline

A real-time data engineering pipeline that streams live cryptocurrency market data through Apache Kafka and Apache Spark, models it into a star schema in MySQL, and visualizes it through an interactive Power BI dashboard.

![Architecture](https://img.shields.io/badge/Architecture-Kafka%20%7C%20Spark%20%7C%20MySQL%20%7C%20PowerBI-blue)
![Python](https://img.shields.io/badge/Python-3.11%20%7C%203.14-blue)
![Status](https://img.shields.io/badge/Status-Active-success)

---

## Overview

This project implements an end-to-end real-time analytics pipeline for cryptocurrency market data. Live prices for **Bitcoin, Ethereum, and Solana** are fetched from the CoinGecko API every 5 seconds, streamed through Kafka, processed with Spark Structured Streaming, and stored in a dimensionally modeled MySQL warehouse for live reporting in Power BI.

```
CoinGecko API → Apache Kafka → Apache Spark Streaming → MySQL (Star Schema) → Power BI
```

---

## Architecture

| Stage | Component | Description |
|---|---|---|
| Source | CoinGecko API | Live market data for BTC, ETH, SOL (in INR) |
| Ingestion | `data_fetcher.py` | Extracts and transforms API data |
| Streaming | `kafka_producer.py` | Publishes records to Kafka every 5 seconds |
| Broker | Apache Kafka (KRaft mode) | Topic: `crypto_prices`, partitioned by coin |
| Processing | `spark_streaming.py` | Spark Structured Streaming, 5-second micro-batches |
| Warehouse | MySQL (`crypto_warehouse`) | Star schema data model |
| Visualization | Power BI | Connects directly to MySQL warehouse |

### Kafka Partition Strategy

The `crypto_prices` topic is keyed and partitioned by `coin_id`, enabling ordered, parallel processing per asset:

| Partition | Coin |
|---|---|
| 0 | Bitcoin |
| 1 | Ethereum |
| 2 | Solana |

---

## Data Warehouse Design (Star Schema)

The warehouse follows a star schema — one central fact table surrounded by four dimension tables — for fast analytical queries.

**Fact Table: `fact_prices`**
Stores current price, market cap, volume, and price change across 1h, 24h, and 7d intervals, linked to all four dimensions.

**Dimension Tables:**

| Table | Purpose |
|---|---|
| `dim_coin` | Coin identity (coin_id, symbol, name) |
| `dim_date` | Calendar attributes — day, month, quarter, weekend flag |
| `dim_time` | Hour-level time bucket and trading session (Asia/Europe/US) |
| `dim_market` | Market cap rank, market cap category, volatility, trend |

```sql
CREATE DATABASE IF NOT EXISTS crypto_warehouse;
USE crypto_warehouse;

CREATE TABLE dim_coin (
    coin_key        INT AUTO_INCREMENT PRIMARY KEY,
    coin_id         VARCHAR(50) UNIQUE,
    symbol          VARCHAR(10),
    name            VARCHAR(100),
    category        VARCHAR(50),
    founded_year    INT,
    blockchain      VARCHAR(50)
);

CREATE TABLE dim_date (
    date_key        INT AUTO_INCREMENT PRIMARY KEY,
    full_date       DATE,
    day             INT,
    month           INT,
    year            INT,
    quarter         INT,
    day_name        VARCHAR(20),
    month_name      VARCHAR(20),
    is_weekend      BOOLEAN
);

CREATE TABLE dim_time (
    time_key        INT AUTO_INCREMENT PRIMARY KEY,
    full_time       TIME,
    hour            INT,
    minute          INT,
    am_pm           VARCHAR(5),
    trading_session VARCHAR(50)
);

CREATE TABLE dim_market (
    market_key          INT AUTO_INCREMENT PRIMARY KEY,
    market_cap_rank     INT,
    market_cap_category VARCHAR(50),
    volatility_level    VARCHAR(20),
    trend               VARCHAR(20)
);

CREATE TABLE fact_prices (
    fact_id             INT AUTO_INCREMENT PRIMARY KEY,
    coin_key            INT,
    date_key            INT,
    time_key            INT,
    market_key          INT,
    current_price       DOUBLE,
    market_cap          BIGINT,
    volume              BIGINT,
    price_change_1h     DOUBLE,
    price_change_24h    DOUBLE,
    price_change_7d     DOUBLE,
    recorded_at         DATETIME,
    FOREIGN KEY (coin_key)   REFERENCES dim_coin(coin_key),
    FOREIGN KEY (date_key)   REFERENCES dim_date(date_key),
    FOREIGN KEY (time_key)   REFERENCES dim_time(time_key),
    FOREIGN KEY (market_key) REFERENCES dim_market(market_key)
);
```

> Dimension rows are upserted at stream time — `spark_streaming.py` looks up each dimension key per record and inserts a new row only if one doesn't already exist (see `get_coin_key`, `get_date_key`, `get_time_key`, `get_market_key`).

---

## Dashboard Preview

**Combined view — all three coins**

![All Coins Dashboard](readme_assets/dashboard_all.jpg)

**Bitcoin live ticker**

![Bitcoin Dashboard](readme_assets/dashboard_bitcoin.jpg)

**Ethereum live ticker**

![Ethereum Dashboard](readme_assets/dashboard_ethereum.jpg)

**Solana live ticker**

![Solana Dashboard](readme_assets/dashboard_solana.jpg)

**Dashboard features:**
- Session high/low price tracking
- Real-time intraday price trend
- 24h returns distribution
- Trading volume by global session (Asia/Europe)
- 24h market sentiment index

---

## Project Structure

```
.
├── data_fetcher.py        # Extracts & transforms live data from CoinGecko API
├── kafka_producer.py      # Publishes coin data to Kafka topic (crypto_prices)
├── kafka_consumer.py      # Kafka consumer (testing/debugging)
├── kafka_setup.txt        # Kafka topic & partition setup notes
├── spark_config.py        # Spark session configuration
├── spark_streaming.py     # Spark Structured Streaming job (Kafka → MySQL)
├── powerbi_push.py        # Optional: direct push to a Power BI streaming dataset (not part of the main pipeline — see note below)
├── workflow.txt           # Pipeline architecture reference
└── README.md
```

> **Note on `powerbi_push.py`:** This script pushes rows from MySQL directly to a Power BI streaming dataset via REST API. It is **not** part of the core project workflow — the main pipeline connects Power BI directly to MySQL. This script is kept as an alternate/experimental path for push-based real-time updates.

---

## Tech Stack

- **Language:** Python
- **Streaming:** Apache Kafka
- **Processing:** Apache Spark (Structured Streaming)
- **Database:** MySQL
- **Visualization:** Power BI
- **Data Source:** CoinGecko API

---

## Setup & Installation

### Prerequisites

- **Java 17.0.8 (JDK)** — required by both Kafka and Spark
- **Python 3.14** — used to run `data_fetcher.py` and `kafka_producer.py`
- **Python 3.11** — used to run `spark_streaming.py` (PySpark does not yet support 3.14)
- Apache Kafka (running in **KRaft mode** — no Zookeeper required)
- Apache Spark
- MySQL Server
- **MySQL Connector/NET 8.0.28** — required for Power BI to connect to MySQL
- Power BI Desktop
- Hadoop winutils (Windows only — required by Spark; set via `HADOOP_HOME`)

> **Important — Python version split:** PySpark currently supports up to Python 3.11, while `kafka-python` runs fine on 3.14. This project uses **two separate virtual environments** — one on Python 3.14 for the producer/fetcher scripts, and one on Python 3.11 for the Spark streaming job. Run each script with its matching environment.

### 1. Clone the Repository

```bash
git clone <https://github.com/Manikandan-23-R/Real-time-Cryptocurrency-Analytics-Pipeline.git>
cd <https://github.com/Manikandan-23-R/Real-time-Cryptocurrency-Analytics-Pipeline>
```

### 2. Set Up Two Virtual Environments

**Environment A — Python 3.14 (for data_fetcher.py & kafka_producer.py)**

```bash
py -3.14 -m venv .venv314
.venv314\Scripts\activate
pip install requests kafka-python
```

**Environment B — Python 3.11 (for spark_streaming.py)**

```bash
py -3.11 -m venv .venv311
.venv311\Scripts\activate
pip install pyspark mysql-connector-python
```

### 3. Install Java 17 and Set JAVA_HOME

Install **JDK 17.0.8**, then set:

```bash
JAVA_HOME = C:\Program Files\Java\jdk-17.0.8
```

### 4. Set Up Hadoop winutils (Windows)

Download `winutils.exe` for Hadoop and place it under `C:\hadoop\bin`. This is already referenced in `spark_streaming.py`:

```python
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")
```

### 5. Start Kafka (KRaft Mode)

This setup runs Kafka in **KRaft mode**, so there's no Zookeeper to start — Kafka manages its own metadata internally.

Generate a cluster ID and format storage (first time only):

```bash
bin\windows\kafka-storage.bat random-uuid
bin\windows\kafka-storage.bat format -t <generated-uuid> -c config\kraft\server.properties
```

Start the Kafka broker:

```bash
bin\windows\kafka-server-start.bat config\kraft\server.properties
```

### 6. Create the Kafka Topic

```bash
bin\windows\kafka-topics.bat --create ^
  --topic crypto_prices ^
  --bootstrap-server localhost:9092 ^
  --partitions 3 ^
  --replication-factor 1
```

### 7. Set Up the MySQL Database

Run the star schema SQL shown above (or save it as `schema.sql`):

```bash
mysql -u root -p < schema.sql
```

Update the `MYSQL_CONFIG` dictionary in `spark_streaming.py` with your own credentials.

### 8. Start the Kafka Producer

*(Activate the Python 3.14 environment first)*

```bash
.venv314\Scripts\activate
python kafka_producer.py
```

This fetches live prices for Bitcoin, Ethereum, and Solana from CoinGecko every 5 seconds and publishes them to the `crypto_prices` topic.

### 9. Run the Spark Streaming Job

*(Activate the Python 3.11 environment first, in a separate terminal)*

```bash
.venv311\Scripts\activate
python spark_streaming.py
```

This consumes the Kafka stream in 5-second micro-batches and writes records into the `fact_prices` table in MySQL, resolving dimension keys along the way.

### 10. Connect Power BI

Power BI requires **MySQL Connector/NET 8.0.28** installed on the machine to connect to a MySQL database (the standard MySQL ODBC driver does not work directly with Power BI's MySQL connector — install Connector/NET first, then restart Power BI Desktop).

1. Open Power BI Desktop
2. **Get Data → MySQL Database**
3. Server: `localhost`, Database: `crypto_warehouse`
4. Select `fact_prices` and all four dimension tables
5. Verify relationships are set on the foreign keys (`coin_key`, `date_key`, `time_key`, `market_key`)
6. Set a refresh interval for near real-time updates

---

## Pipeline Flow Summary

1. `data_fetcher.py` calls the CoinGecko API for Bitcoin, Ethereum, and Solana prices (in INR) and transforms the response into clean event records
2. `kafka_producer.py` runs every 5 seconds, sending each coin's event to the `crypto_prices` topic, keyed by `coin_id`
3. `spark_streaming.py` reads from Kafka using Spark Structured Streaming, processing in 5-second micro-batches via `foreachBatch`
4. For each record, dimension keys are resolved or created (`dim_coin`, `dim_date`, `dim_time`, `dim_market`), and a new row is inserted into `fact_prices`
5. Power BI connects directly to the `crypto_warehouse` MySQL database and refreshes to display live metrics on the dashboard

---

## Future Improvements

- Add more cryptocurrencies and configurable coin lists
- Implement automated alerting for significant price movements
- Containerize the stack with Docker Compose (Kafka, Spark, MySQL)
- Unify the pipeline on a single Python version once PySpark adds 3.14 support
- Add a CI/CD pipeline for deployment

---

## Author

**Manikandan R**
Aspiring Data Analyst & Data Engineer | Python | MySQL | Data Engineering
Chennai, Tamil Nadu

[LinkedIn](https://www.linkedin.com/in/manikandan-r) · [GitHub](https://github.com/<your-username>)
