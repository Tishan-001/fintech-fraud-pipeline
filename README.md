# 🛡️ LankaPay Fraud Detection Pipeline

> **A production-grade Lambda Architecture for real-time financial fraud detection** — built with Kafka, Spark Structured Streaming, Airflow, and PostgreSQL, containerised end-to-end with Docker Compose.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SPEED LAYER (Real-Time)                      │
│                                                                     │
│  [Python Producer] ──► [Kafka: transactions] ──► [Spark Streaming]  │
│                                                       │        │    │
│                                              [Fraud] [Clean]        │
│                                                 │        │          │
│                                          [PostgreSQL] [Parquet]     │
│                                          fraud_alerts  /data/       │
│                                                        warehouse/   │
└─────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────┐
│                       BATCH LAYER (Every 6h)                        │
│                                                                     │
│  [Airflow DAG] ──► validate ──► ingress ──► validate ──► report     │
│                                                    │                │
│                                          [/reports/*.csv]           │
│                                          [reconciliation_log]       │
└─────────────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────────────┐
│                    ANALYTIC LAYER (On-demand)                       │
│                                                                     │
│  [generate_analytic_report.py] ──► fraud_by_merchant.png + .csv     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🧰 Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Message Broker** | Apache Kafka 7.5 + Zookeeper | Durable, partitioned transaction stream |
| **Stream Processing** | Apache Spark 3.5.1 Structured Streaming | Stateful fraud detection with event-time watermarking |
| **Storage — Hot** | PostgreSQL 15 | Fraud alerts + reconciliation log |
| **Storage — Cold** | Parquet (date-partitioned) | Clean transaction data warehouse |
| **Orchestration** | Apache Airflow 2.8 | 6-hourly batch reconciliation DAG |
| **Analytics** | Python + matplotlib + pandas | Fraud-by-category bar charts |
| **Infrastructure** | Docker Compose | One-command full stack |

---

## 📁 Project Structure

```
fintech-fraud-pipeline/
├── docker-compose.yml               # Full stack — 9 services
├── pg-init.sh                       # Creates airflow DB on first start
├── init_db.sql                      # fraud_alerts + reconciliation_log tables
│
├── producers/
│   └── transaction_producer.py      # Synthetic Kafka transaction generator
│
├── spark_jobs/
│   ├── fraud_detector.py            # Spark Structured Streaming job
│   └── generate_analytic_report.py  # Phase 5 — matplotlib chart generator
│
├── airflow/
│   └── dags/
│       └── reconciliation_dag.py    # 6-hourly Lambda batch reconciliation
│
└── reports/                         # Generated outputs land here
    ├── fraud_by_merchant.csv
    ├── fraud_by_merchant.png
    ├── fraud_summary.txt
    └── reconciliation_YYYYMMDD_HHMM.csv
```

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop ≥ 4.x
- Docker Compose v2
- Python 3.9+ with `pip` (for the producer, run on host)

### 1 — Clone & Start the Stack

```bash
git clone https://github.com/<your-username>/fintech-fraud-pipeline.git
cd fintech-fraud-pipeline
docker-compose up -d
```

Wait ~60 seconds for all health checks to pass. Verify:

```bash
docker-compose ps
```

All containers should show `healthy` or `exited (0)` (for one-shot init containers).

### 2 — Start the Kafka Producer

In a separate terminal:

```bash
pip install kafka-python
python producers/transaction_producer.py
```

You'll see live output:
```
  [NORMAL]        U1008 |    $234.50 | LK   | groceries
  [FRAUD_INJECT]  U1013 |  $8,742.00 | RU   | electronics    | ⚠  HIGH_VALUE
  [FRAUD_INJECT]  U1005 |    $312.40 | LK   | travel         | ⚠  IMPOSSIBLE_TRAVEL → LK then CN
```

### 3 — Submit the Spark Fraud Detector

```bash
docker exec lankapay-spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0 \
  /opt/spark_jobs/fraud_detector.py
```

Spark will begin writing:
- Fraud transactions → PostgreSQL `fraud_alerts`
- Clean transactions → `/data/warehouse/date=YYYY-MM-DD/` (Parquet)

### 4 — Trigger the Airflow Reconciliation DAG

Open **http://localhost:8085** — login: `admin / admin123`

1. Find `lankapay_reconciliation` in the DAG list
2. Toggle it **ON**
3. Click ▶ **Trigger DAG**

The DAG runs 5 tasks in sequence and writes a reconciliation CSV to `/reports/`.

### 5 — Generate the Analytic Report

```bash
docker exec lankapay-spark-master \
  python3 /opt/spark_jobs/generate_analytic_report.py
```

Copy the chart to your local machine:

```bash
docker cp lankapay-spark-master:/reports/fraud_by_merchant.png ./reports/
```

---

## 🔍 Fraud Detection Rules

### Rule 1 — High-Value Transaction
```python
df.filter(col("amount") > 5000)
```
Any single transaction exceeding **$5,000** is immediately flagged as `HIGH_VALUE` and written to PostgreSQL.

### Rule 2 — Impossible Travel
```python
watermarked \
  .groupBy(window(col("timestamp"), "10 minutes"), col("user_id")) \
  .agg(collect_set("location").alias("locations")) \
  .filter(size(col("locations")) > 1)
```
If the same `user_id` appears with **two different country codes** within a **10-minute event-time window**, it is flagged as `IMPOSSIBLE_TRAVEL`. Uses Spark's `withWatermark` to correctly handle late-arriving data.

---

## 🗃️ Database Schema

```sql
-- Written by Spark Streaming
CREATE TABLE fraud_alerts (
    id                SERIAL PRIMARY KEY,
    user_id           VARCHAR(20)    NOT NULL,
    timestamp         TIMESTAMPTZ    NOT NULL,
    merchant_category VARCHAR(50),
    amount            NUMERIC(12, 2) NOT NULL,
    location          VARCHAR(10),
    fraud_type        VARCHAR(50)    NOT NULL,   -- HIGH_VALUE | IMPOSSIBLE_TRAVEL
    detected_at       TIMESTAMPTZ    DEFAULT NOW()
);

-- Written by Airflow DAG
CREATE TABLE reconciliation_log (
    id                   SERIAL PRIMARY KEY,
    period_start         TIMESTAMPTZ   NOT NULL,
    period_end           TIMESTAMPTZ   NOT NULL,
    total_ingress_amount NUMERIC(18,2),
    validated_amount     NUMERIC(18,2),
    fraud_amount         NUMERIC(18,2),
    fraud_rate_pct       NUMERIC(6,2),
    created_at           TIMESTAMPTZ   DEFAULT NOW()
);
```

---

## 🔄 Airflow DAG — Reconciliation Pipeline

Schedule: `0 */6 * * *` (every 6 hours)

```
t1: validate_parquet_exists        ← checks /data/warehouse for partitions
        ↓
t2: calculate_total_ingress        ← Parquet clean txns + PostgreSQL fraud
        ↓
t3: calculate_validated_amount     ← secondary quality pass on Parquet
        ↓
t4: generate_reconciliation_report ← writes CSV + inserts to reconciliation_log
        ↓
t5: log_completion                 ← prints summary to Airflow task log
```

Sample output:
```
╔══════════════════════════════════════════════════════════════╗
  LankaPay Reconciliation — Run Complete
  Window  : 2026-05-08 18:00 → 00:00 UTC
  Total Ingress     : $   75,315.55
  Validated Amount  : $   58,636.30
  Fraud Amount      : $   16,679.25   (22.15% fraud rate)
  Report saved to   : /reports/reconciliation_20260508_1800.csv
╚══════════════════════════════════════════════════════════════╝
```

---

## 🌐 Service Endpoints

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8085 | `admin / admin123` |
| Spark Master UI | http://localhost:8080 | — |
| Spark Worker UI | http://localhost:8081 | — |
| PostgreSQL | `localhost:5432` | `lankapay / lankapay123 / lankapay_db` |
| Kafka | `localhost:9092` | — |

---

## ⚙️ Configuration

Key constants are defined at the top of each file for easy tuning:

| File | Variable | Default | Description |
|---|---|---|---|
| `fraud_detector.py` | `FRAUD_THRESHOLD` | `5000.0` | High-value rule threshold ($) |
| `fraud_detector.py` | `TRAVEL_WINDOW_MIN` | `"10 minutes"` | Impossible travel window |
| `fraud_detector.py` | `WATERMARK_DELAY` | `"10 minutes"` | Late data tolerance |
| `transaction_producer.py` | `SLEEP_BETWEEN_MESSAGES` | `0.5` | Seconds between messages |
| `reconciliation_dag.py` | `schedule_interval` | `0 */6 * * *` | DAG run frequency |

---

## 🧹 Teardown

```bash
# Stop all containers, preserve volumes (data survives)
docker-compose down

# Stop AND delete all data volumes (full reset)
docker-compose down -v
```

---

## 📊 Key Design Decisions

**Event Time vs Processing Time** — Spark uses the `timestamp` field embedded in the Kafka JSON payload as event time, not Spark's ingestion time. Combined with `withWatermark`, this ensures late-arriving messages (e.g. from network delays) are still correctly assigned to their original 10-minute window rather than being silently dropped or misclassified.

**Kafka Partitioning by `user_id`** — The producer sets `user_id` as the Kafka message key. This guarantees all transactions from the same user land on the same partition, which is critical for Spark's stateful impossible-travel groupBy to see both legs of the trip in the same task.

**Lambda Architecture separation** — The speed layer (Spark) optimises for latency; the batch layer (Airflow) optimises for completeness and correctness. The reconciliation DAG deliberately re-reads both the Parquet store and the PostgreSQL fraud table to produce a unified picture that neither layer could produce alone.
