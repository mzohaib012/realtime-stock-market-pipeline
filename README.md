# 📈 Real-Time Stock Market Data Pipeline

An end-to-end, real-time data engineering pipeline that streams live stock prices from **PSX (Pakistan Stock Exchange)** and **NASDAQ**, processes them through a modern streaming stack, and surfaces the results in interactive dashboards.

---

## 🏗️ Architecture
Data Sources (PSX + NASDAQ)
│
▼
Kafka Producer  ──────►  Kafka (KRaft mode)
│                         │
│                         ▼
│                 Kafka Consumer
│                         │
│                         ▼
│              TimescaleDB (time-series storage)
│                         │
│                         ▼
│                  dbt (transformations)
│                         │
▼                         ▼
Airflow DAGs  ──────►  FastAPI (serving layer)
│
▼
Power BI Dashboard

## ✨ Features

- **Real-time ingestion** of PSX and NASDAQ stock prices via a Python producer
- **Kafka (KRaft mode)** — no Zookeeper dependency, lightweight and modern
- **Time-series storage** in TimescaleDB, optimized for high-frequency price data
- **Orchestration** via Apache Airflow DAGs for scheduling and pipeline monitoring
- **Data transformations** using dbt for clean, tested, analytics-ready models
- **REST API layer** built with FastAPI to serve processed data
- **Interactive dashboards** in Power BI for live price tracking and trend analysis

## 🛠️ Tech Stack

| Layer | Tool |
|---|---|
| Streaming | Apache Kafka (KRaft) |
| Ingestion | Python (`kafka-python`, `yfinance`) |
| Storage | TimescaleDB (PostgreSQL extension) |
| Transformation | dbt |
| Orchestration | Apache Airflow |
| API | FastAPI |
| Visualization | Power BI |

## 📂 Project Structure
stock-market-pipeline/
├── producer/
│   └── stock_producer.py       # Fetches & publishes stock data to Kafka
├── consumer/
│   └── stock_consumer.py       # Consumes from Kafka, writes to TimescaleDB
├── orchestration/
│   └── dags/
│       └── stock_pipeline_dag.py   # Airflow DAG for pipeline scheduling
├── api/
│   └── main.py                 # FastAPI app serving processed data
└── .gitignore

## 🚀 How It Works

1. **Producer** fetches live stock prices for tracked PSX and NASDAQ tickers and publishes them to a Kafka topic.
2. **Consumer** subscribes to the topic, validates and enriches the data, and writes it into TimescaleDB.
3. **Airflow** orchestrates the pipeline on a schedule — triggering fetch cycles, monitoring health, and handling retries.
4. **dbt** transforms raw price data into clean, analytics-ready tables (e.g., daily aggregates, moving averages).
5. **FastAPI** exposes REST endpoints to query the latest and historical stock data.
6. **Power BI** connects to the database/API to visualize price trends in real time.

## ⚙️ Setup & Running Locally

> **Prerequisites:** Python 3.10+, Docker, Kafka, PostgreSQL/TimescaleDB

```bash
git clone https://github.com/mzohaib012/realtime-stock-market-pipeline.git
cd realtime-stock-market-pipeline

python -m venv venv
venv\Scripts\activate

pip install -r requirements.txt

python producer/stock_producer.py
python consumer/stock_consumer.py
uvicorn api.main:app --reload
```

## 📊 Sample Output
✅ AAPL   | Price: 214.32 | Exchange: NASDAQ
✅ TSLA   | Price: 375.53 | Exchange: NASDAQ
✅ LUCK   | Price:  921.10 | Exchange: PSX

## 🔮 Future Improvements

- [ ] Add Docker Compose for one-command spin-up of the full stack
- [ ] Implement alerting for significant price movements
- [ ] Add unit and integration tests
- [ ] Deploy to cloud (AWS/GCP) with managed Kafka and Airflow

## 👤 Author

**Zohaib** — Data Engineer
📍 Lahore, Pakistan
🔗 [LinkedIn](#) | [GitHub](https://github.com/mzohaib012)

---

*Part of a broader portfolio of data engineering projects, including an E-Commerce Sales Analytics Pipeline and a FIFA World Cup 2026 end-to-end data platform.*