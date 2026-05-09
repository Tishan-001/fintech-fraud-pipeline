# 🛡️ LankaPay Fraud Detection Pipeline

> **A production-grade Lambda Architecture for real-time financial fraud detection** — built with Kafka, Spark Structured Streaming, Airflow, and PostgreSQL, containerised end-to-end with Docker Compose.

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                        SPEED LAYER (Real-Time)                               │
│                                                                              │ 
│  [Python Producer] ──► [Kafka: transactions] ──► [Spark Streaming]           │
│                                                       │        │             │
│                                                    [Fraud] [Clean]           │
│                                                       │        │             │
│                                                  [PostgreSQL] [Parquet]      │
│                                                   fraud_alerts  /data/       │
│                                                               warehouse/     │
└──────────────────────────────────────────────────────────────────────────────┘
┌──────────────────────────────────────────────────────────────────────────────┐
│                       BATCH LAYER (Every 6h)                                 │
│                                                                              │
│  [Airflow DAG] ──► validate ──► ingress ──► validate ──► report              │
│                                                            │                 │
│                                                   [/reports/*.csv]           │
│                                                 [reconciliation_log]         │
└──────────────────────────────────────────────────────────────────────────────┘
┌──────────────────────────────────────────────────────────────────────────────┐
│                    ANALYTIC LAYER (On-demand)                                │
│                                                                              │
│  [generate_analytic_report.py] ──► fraud_by_merchant.png + .csv              │
└──────────────────────────────────────────────────────────────────────────────┘
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
├── docker-compose.yml               # Full stack — 11 services
├── pg-init.sh                       # Creates airflow DB on first start
├── init_db.sql                      # fraud_alerts + reconciliation_log tables
│
├── producers/
│   ├── transaction_producer.py      # Synthetic Kafka transaction generator
│   └── fraud_alert_watcher.py       # Real-time fraud alert console monitor
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
- Python 3.9+ with `pip` (for host-side scripts)

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

Open **Terminal 1**:

```bash
pip install kafka-python
python producers/transaction_producer.py
```

You'll see live output:
```
  [NORMAL]        U1008 |  LKR   23,450.00 | LK   | groceries
  [FRAUD_INJECT]  U1013 |  LKR  274,200.00 | RU   | electronics    | ⚠  HIGH_VALUE (amount > LKR 50,000)
  [FRAUD_INJECT]  U1005 |  LKR   31,240.00 | LK   | travel         | ⚠  IMPOSSIBLE_TRAVEL → LK then CN
```

### 3 — Start the Real-Time Fraud Alert Watcher

Open **Terminal 2** — runs alongside the producer and fires alerts within 5 seconds of Spark writing to PostgreSQL:

```bash
pip install psycopg2-binary
python producers/fraud_alert_watcher.py --local
```

When fraud is detected you will see:

```
══════════════════════════════════════════════════════════════
  ⚠  FRAUD ALERT  —  HIGH-VALUE TRANSACTION DETECTED  💸
══════════════════════════════════════════════════════════════
  Alert ID       : 42
  User           : U1013
  Fraud Type     : HIGH_VALUE
  Amount         : LKR 274,200.00
  Location       : RU
  Merchant Cat.  : electronics
  Event Time     : 2026-05-08 13:22:11+00:00
  Detected At    : 2026-05-08 13:22:14+00:00
══════════════════════════════════════════════════════════════
```

Between alerts the watcher prints a live heartbeat:
```
  [13:22:45]  Watching...  (alerts fired so far: 3)
```

### 4 — Submit the Spark Fraud Detector

Open **Terminal 3**:

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

### 5 — Trigger the Airflow Reconciliation DAG

Open **http://localhost:8085** — login: `admin / admin123`

1. Find `lankapay_reconciliation` in the DAG list
2. Toggle it **ON**
3. Click ▶ **Trigger DAG**

The DAG runs 6 tasks in sequence. The first task (`check_and_alert_fraud`) immediately prints a fraud bulletin for the current window before the reconciliation work begins.

### 6 — Generate the Analytic Report

```bash
docker exec lankapay-spark-master \
  python3 /opt/spark_jobs/generate_analytic_report.py
```

Copy the chart to your local machine:

```bash
docker cp lankapay-spark-master:/reports/fraud_by_merchant.png ./reports/
```

---

## 🚨 Fraud Alerting — Two Layers

The pipeline provides alerts at two different timescales:

| Method | Latency | Where to see it |
|---|---|---|
| `fraud_alert_watcher.py` | **~5 seconds** after fraud hits PostgreSQL | Terminal running the watcher |
| Airflow `t0: check_and_alert_fraud` | **Every 6 hours** — full window bulletin | Airflow UI → task log |

The watcher is for **immediate operational response**. The Airflow `t0` bulletin is a **structured audit record** of all fraud in a 6-hour period, sorted by amount, preserved permanently in Airflow task log history.

---

## 🔍 Fraud Detection Rules

### Rule 1 — High-Value Transaction
```python
df.filter(col("amount") > 50000)   # LKR 50,000
```
Any single transaction exceeding **LKR 50,000** is immediately flagged as `HIGH_VALUE` and written to PostgreSQL.

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
    amount            NUMERIC(12, 2) NOT NULL,   -- stored in LKR
    location          VARCHAR(10),
    fraud_type        VARCHAR(50)    NOT NULL,   -- HIGH_VALUE | IMPOSSIBLE_TRAVEL
    detected_at       TIMESTAMPTZ    DEFAULT NOW()
);

-- Written by Airflow DAG
CREATE TABLE reconciliation_log (
    id                   SERIAL PRIMARY KEY,
    period_start         TIMESTAMPTZ   NOT NULL,
    period_end           TIMESTAMPTZ   NOT NULL,
    total_ingress_amount NUMERIC(18,2),           -- stored in LKR
    validated_amount     NUMERIC(18,2),           -- stored in LKR
    fraud_amount         NUMERIC(18,2),           -- stored in LKR
    fraud_rate_pct       NUMERIC(6,2),
    created_at           TIMESTAMPTZ   DEFAULT NOW()
);
```

---

## 🔄 Airflow DAG — Reconciliation Pipeline

Schedule: `0 */6 * * *` (every 6 hours)

```
t0: check_and_alert_fraud          ← fraud bulletin for the window
        ↓
t1: validate_parquet_exists        ← checks /data/warehouse for partitions
        ↓
t2: calculate_total_ingress        ← Parquet clean txns + PostgreSQL fraud
        ↓
t3: calculate_validated_amount     ← secondary quality pass on Parquet
        ↓
t4: generate_reconciliation_report ← writes CSV + inserts to reconciliation_log
        ↓
t5: log_completion                 ← prints summary including alert count
```

Sample `t0` output when fraud is detected (visible in Airflow task log):
```
================================================================
  *** FRAUD ALERT BULLETIN — 3 INCIDENT(S) DETECTED ***
  Window             : 2026-05-08 12:00 -> 18:00 UTC
  Total Fraud Amount : LKR 1,243,500.00
  High-Value         : 3 incident(s)  (> LKR 50,000)
  Impossible Travel  : 0 incident(s)
================================================================

  Alert 1/3  [HIGH-VALUE > LKR 50,000]
  ----------------------------------------------------------------
  Alert ID      : 42
  User          : U1013
  Amount        : LKR 274,200.00
  Location      : RU
  Merchant Cat. : electronics
  Event Time    : 2026-05-08 13:22:11+00:00
  Detected At   : 2026-05-08 13:22:14+00:00
```

Sample `t5` completion summary:
```
================================================================
  LankaPay Reconciliation -- Run Complete
  Window  : 2026-05-08 18:00 -> 00:00 UTC
  ----------------------------------------------------------------
  Total Ingress     : LKR  7,531,550.00
  Validated Amount  : LKR  5,863,630.00
  Fraud Amount      : LKR  1,667,920.00   (22.15% fraud rate)
  ----------------------------------------------------------------
  Fraud Alerts Fired : 3 incident(s)  /  LKR 1,243,500.00 total
  ----------------------------------------------------------------
  Report saved to   : /reports/reconciliation_20260508_1800.csv
================================================================
```

---

## 🌐 Service Endpoints

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8085 | `admin / admin123` |
| Spark Master UI | http://localhost:8080 | — |
| Spark Worker UI | http://localhost:8081 | — |
| PostgreSQL | `localhost:5432` | `lankapay_db` |
| Kafka | `localhost:9092` | — |

---

## ⚙️ Configuration

Key constants are defined at the top of each file for easy tuning:

| File | Variable | Default | Description |
|---|---|---|---|
| `fraud_detector.py` | `FRAUD_THRESHOLD` | `50000.0` | High-value rule threshold (LKR) |
| `fraud_detector.py` | `TRAVEL_WINDOW_MIN` | `"10 minutes"` | Impossible travel window |
| `fraud_detector.py` | `WATERMARK_DELAY` | `"10 minutes"` | Late data tolerance |
| `transaction_producer.py` | `SLEEP_BETWEEN_MESSAGES` | `0.5` | Seconds between messages |
| `fraud_alert_watcher.py` | `POLL_INTERVAL_SEC` | `5` | Watcher polling frequency (seconds) |
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

**Kafka Partitioning by `user_id`** — The producer sets `user_id` as the Kafka message key. This guarantees all transactions from the same user land on the same partition, which is critical for Spark's stateful impossible-travel `groupBy` to see both legs of the trip in the same task.

**Dual-sink routing** — Fraud alerts (low volume, high importance) go to PostgreSQL for ACID-guaranteed persistence and immediate queryability. Clean transactions (high volume) go to date-partitioned Parquet, enabling efficient predicate pushdown when Airflow reads a single 6-hour window.

**Lambda Architecture separation** — The speed layer (Spark) optimises for latency; the batch layer (Airflow) optimises for completeness and correctness. The reconciliation DAG deliberately re-reads both the Parquet store and the PostgreSQL fraud table to produce a unified picture that neither layer could produce alone.

**Two-tier alerting** — The `fraud_alert_watcher.py` provides operational response within seconds by polling PostgreSQL continuously. The Airflow `t0` task provides a structured, auditable fraud bulletin every 6 hours that is preserved in Airflow's task log history alongside the reconciliation numbers.
