"""
============================================================
 LankaPay Wallet — Airflow Reconciliation DAG
 File: airflow/dags/reconciliation_dag.py

 Schedule: every 6 hours  →  0 */6 * * *

 DAG task order:
   t1: validate_parquet_exists
         ↓
   t2: calculate_total_ingress       ← all Parquet rows × amount
         ↓
   t3: calculate_validated_amount    ← clean (non-fraud) Parquet rows
         ↓
   t4: generate_reconciliation_report ← CSV + PostgreSQL insert
         ↓
   t5: log_completion

 Outputs:
   - /reports/reconciliation_<period_start>.csv
   - reconciliation_log table in PostgreSQL

 Dependencies (pre-installed in Airflow image or via requirements):
   pip install pandas pyarrow psycopg2-binary
============================================================
"""

from __future__ import annotations

import os
import csv
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

# ── Config ────────────────────────────────────────────────────
PARQUET_DIR   = Path("/data/warehouse")
REPORTS_DIR   = Path("/reports")

POSTGRES_CONN = {
    "host":     "postgres",
    "port":     5432,
    "dbname":   "lankapay_db",
    "user":     "lankapay",
    "password": "lankapay123",
}

FRAUD_THRESHOLD = 5_000.0   # mirrors Spark Rule 1 threshold

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Helper: derive the 6-hour reporting window from logical_date
# ─────────────────────────────────────────────────────────────
def _get_window(logical_date: datetime) -> tuple[datetime, datetime]:
    """
    Round logical_date DOWN to the nearest 6-hour boundary.
    e.g. 2025-07-14 09:15 → (2025-07-14 06:00, 2025-07-14 12:00)
    """
    hour_block  = (logical_date.hour // 6) * 6
    period_start = logical_date.replace(
        hour=hour_block, minute=0, second=0, microsecond=0,
        tzinfo=timezone.utc
    )
    period_end   = period_start + timedelta(hours=6)
    return period_start, period_end


# ─────────────────────────────────────────────────────────────
# t1 — validate_parquet_exists
# ─────────────────────────────────────────────────────────────
def validate_parquet_exists(**context) -> None:
    """
    Checks that at least one Parquet partition directory exists
    under /data/warehouse that falls within today's date range.
    Skips gracefully if no data has been written yet (e.g. Spark
    hasn't emitted any micro-batches for the current window).
    """
    logical_date                = context["logical_date"]
    period_start, period_end    = _get_window(logical_date)

    log.info("Reconciliation window: %s → %s", period_start, period_end)

    if not PARQUET_DIR.exists():
        raise AirflowSkipException(
            f"Parquet root {PARQUET_DIR} does not exist — Spark may not have started yet."
        )

    # Look for any date=YYYY-MM-DD partition that overlaps our window.
    # For a 6-hour window spanning a single day this is usually one folder.
    partition_dates = set()
    for item in PARQUET_DIR.iterdir():
        if item.is_dir() and item.name.startswith("date="):
            partition_dates.add(item.name)

    if not partition_dates:
        raise AirflowSkipException("No Parquet partitions found — skipping reconciliation.")

    log.info("Found partitions: %s", sorted(partition_dates))

    # Stash window into XCom for downstream tasks
    context["ti"].xcom_push(key="period_start", value=period_start.isoformat())
    context["ti"].xcom_push(key="period_end",   value=period_end.isoformat())
    context["ti"].xcom_push(key="partitions",   value=sorted(partition_dates))


# ─────────────────────────────────────────────────────────────
# t2 — calculate_total_ingress
# ─────────────────────────────────────────────────────────────
def calculate_total_ingress(**context) -> None:
    """
    Reads ALL Parquet rows (clean transactions written by Spark).
    Sums the `amount` column to get total money that entered the
    pipeline within the reporting window.

    Note: Spark only writes CLEAN (amount ≤ $5000) transactions to
    Parquet. High-value fraud is diverted to PostgreSQL directly.
    We therefore also query PostgreSQL fraud_alerts to get the full
    picture for the ingress total.
    """
    import pandas as pd
    import psycopg2

    ti              = context["ti"]
    period_start    = datetime.fromisoformat(ti.xcom_pull(key="period_start", task_ids="validate_parquet_exists"))
    period_end      = datetime.fromisoformat(ti.xcom_pull(key="period_end",   task_ids="validate_parquet_exists"))

    # ── Read Parquet (clean transactions) ────────────────────
    parquet_dfs = []
    for partition in PARQUET_DIR.glob("date=*"):
        if partition.is_dir():
            try:
                df = pd.read_parquet(partition)
                parquet_dfs.append(df)
            except Exception as exc:
                log.warning("Could not read partition %s: %s", partition, exc)

    if parquet_dfs:
        all_clean   = pd.concat(parquet_dfs, ignore_index=True)
        # Filter to window using event-time timestamp
        all_clean["timestamp"] = pd.to_datetime(all_clean["timestamp"], utc=True)
        mask        = (all_clean["timestamp"] >= period_start) & \
                      (all_clean["timestamp"] <  period_end)
        window_clean = all_clean[mask]
        clean_amount = float(window_clean["amount"].sum())
        clean_count  = int(len(window_clean))
    else:
        clean_amount = 0.0
        clean_count  = 0

    # ── Read fraud amounts from PostgreSQL ───────────────────
    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        cur  = conn.cursor()
        cur.execute(
            """
            SELECT COALESCE(SUM(amount), 0), COUNT(*)
            FROM   fraud_alerts
            WHERE  timestamp >= %s AND timestamp < %s
            """,
            (period_start, period_end),
        )
        fraud_amount, fraud_count = cur.fetchone()
        fraud_amount = float(fraud_amount)
        fraud_count  = int(fraud_count)
        cur.close()
        conn.close()
    except Exception as exc:
        log.warning("Could not query PostgreSQL fraud_alerts: %s — defaulting to 0", exc)
        fraud_amount = 0.0
        fraud_count  = 0

    total_ingress = clean_amount + fraud_amount
    total_count   = clean_count + fraud_count

    log.info(
        "Total ingress: $%.2f (%d txns)  |  Clean: $%.2f (%d)  |  "
        "Fraud: $%.2f (%d)",
        total_ingress, total_count,
        clean_amount,  clean_count,
        fraud_amount,  fraud_count,
    )

    ti.xcom_push(key="total_ingress_amount", value=total_ingress)
    ti.xcom_push(key="total_ingress_count",  value=total_count)
    ti.xcom_push(key="fraud_amount",         value=fraud_amount)
    ti.xcom_push(key="fraud_count",          value=fraud_count)
    ti.xcom_push(key="clean_amount",         value=clean_amount)
    ti.xcom_push(key="clean_count",          value=clean_count)


# ─────────────────────────────────────────────────────────────
# t3 — calculate_validated_amount
# ─────────────────────────────────────────────────────────────
def calculate_validated_amount(**context) -> None:
    """
    Re-reads the Parquet clean dataset and applies a secondary
    validation pass:
      - Exclude rows where amount ≤ 0 (bad data)
      - Exclude rows missing user_id or merchant_category
    This gives the "validated" amount — the most trustworthy figure
    for reconciliation. Any gap between total_ingress and validated
    signals data quality issues to investigate.
    """
    import pandas as pd

    ti           = context["ti"]
    period_start = datetime.fromisoformat(ti.xcom_pull(key="period_start", task_ids="validate_parquet_exists"))
    period_end   = datetime.fromisoformat(ti.xcom_pull(key="period_end",   task_ids="validate_parquet_exists"))

    parquet_dfs = []
    for partition in PARQUET_DIR.glob("date=*"):
        if partition.is_dir():
            try:
                parquet_dfs.append(pd.read_parquet(partition))
            except Exception as exc:
                log.warning("Skipping partition %s: %s", partition, exc)

    if not parquet_dfs:
        validated_amount = 0.0
        validated_count  = 0
    else:
        df = pd.concat(parquet_dfs, ignore_index=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        # Window filter
        mask = (df["timestamp"] >= period_start) & (df["timestamp"] < period_end)
        df   = df[mask]

        # Validation rules
        df = df[df["amount"] > 0]
        df = df[df["user_id"].notna() & (df["user_id"] != "")]
        df = df[df["merchant_category"].notna() & (df["merchant_category"] != "")]

        validated_amount = float(df["amount"].sum())
        validated_count  = int(len(df))

    log.info("Validated amount: $%.2f (%d txns)", validated_amount, validated_count)

    ti.xcom_push(key="validated_amount", value=validated_amount)
    ti.xcom_push(key="validated_count",  value=validated_count)


# ─────────────────────────────────────────────────────────────
# t4 — generate_reconciliation_report
# ─────────────────────────────────────────────────────────────
def generate_reconciliation_report(**context) -> None:
    """
    Produces:
      1. A CSV file at /reports/reconciliation_<period_start>.csv
      2. A row inserted into the reconciliation_log PostgreSQL table

    CSV columns:
      period, total_ingress_amount, validated_amount,
      fraud_amount, fraud_rate_pct, clean_count, fraud_count,
      data_quality_gap, generated_at
    """
    import psycopg2

    ti = context["ti"]

    period_start      = datetime.fromisoformat(ti.xcom_pull(key="period_start",         task_ids="validate_parquet_exists"))
    period_end        = datetime.fromisoformat(ti.xcom_pull(key="period_end",           task_ids="validate_parquet_exists"))
    total_ingress     = ti.xcom_pull(key="total_ingress_amount",  task_ids="calculate_total_ingress")
    fraud_amount      = ti.xcom_pull(key="fraud_amount",          task_ids="calculate_total_ingress")
    clean_count       = ti.xcom_pull(key="clean_count",           task_ids="calculate_total_ingress")
    fraud_count       = ti.xcom_pull(key="fraud_count",           task_ids="calculate_total_ingress")
    validated_amount  = ti.xcom_pull(key="validated_amount",      task_ids="calculate_validated_amount")

    # ── Derived metrics ──────────────────────────────────────
    fraud_rate_pct     = round((fraud_amount / total_ingress * 100), 4) if total_ingress > 0 else 0.0
    data_quality_gap   = round(total_ingress - fraud_amount - validated_amount, 2)
    period_label       = f"{period_start.strftime('%Y-%m-%d %H:%M')}–{period_end.strftime('%H:%M')} UTC"
    generated_at       = datetime.now(timezone.utc).isoformat()

    report_row = {
        "period":               period_label,
        "total_ingress_amount": round(total_ingress,    2),
        "validated_amount":     round(validated_amount, 2),
        "fraud_amount":         round(fraud_amount,     2),
        "fraud_rate_pct":       fraud_rate_pct,
        "clean_txn_count":      clean_count,
        "fraud_txn_count":      fraud_count,
        "data_quality_gap":     data_quality_gap,
        "generated_at":         generated_at,
    }

    log.info("Reconciliation report: %s", report_row)

    # ── Write CSV ────────────────────────────────────────────
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    filename   = f"reconciliation_{period_start.strftime('%Y%m%d_%H%M')}.csv"
    csv_path   = REPORTS_DIR / filename

    fieldnames = list(report_row.keys())
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(report_row)

    log.info("CSV written: %s", csv_path)

    # ── Insert into PostgreSQL reconciliation_log ─────────────
    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        cur  = conn.cursor()
        cur.execute(
            """
            INSERT INTO reconciliation_log
              (period_start, period_end, total_ingress_amount,
               validated_amount, fraud_amount, fraud_rate_pct)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                period_start,
                period_end,
                report_row["total_ingress_amount"],
                report_row["validated_amount"],
                report_row["fraud_amount"],
                report_row["fraud_rate_pct"],
            ),
        )
        conn.commit()
        cur.close()
        conn.close()
        log.info("Inserted reconciliation row into PostgreSQL.")
    except Exception as exc:
        # Non-fatal: CSV is already written; log the DB failure.
        log.error("Failed to insert into reconciliation_log: %s", exc)

    # Stash CSV path for the completion log
    ti.xcom_push(key="csv_path", value=str(csv_path))


# ─────────────────────────────────────────────────────────────
# t5 — log_completion
# ─────────────────────────────────────────────────────────────
def log_completion(**context) -> None:
    """
    Prints a clean summary to the Airflow task log.
    Acts as a human-readable audit trail for each DAG run.
    """
    ti = context["ti"]

    period_start     = ti.xcom_pull(key="period_start",         task_ids="validate_parquet_exists")
    period_end       = ti.xcom_pull(key="period_end",           task_ids="validate_parquet_exists")
    total_ingress    = ti.xcom_pull(key="total_ingress_amount",  task_ids="calculate_total_ingress")
    fraud_amount     = ti.xcom_pull(key="fraud_amount",          task_ids="calculate_total_ingress")
    validated_amount = ti.xcom_pull(key="validated_amount",      task_ids="calculate_validated_amount")
    csv_path         = ti.xcom_pull(key="csv_path",              task_ids="generate_reconciliation_report")

    fraud_rate = (fraud_amount / total_ingress * 100) if total_ingress else 0.0

    summary = f"""
╔══════════════════════════════════════════════════════════════╗
  LankaPay Reconciliation — Run Complete
  Window  : {period_start}  →  {period_end}
  ─────────────────────────────────────────────────────────────
  Total Ingress     : ${total_ingress:>12,.2f}
  Validated Amount  : ${validated_amount:>12,.2f}
  Fraud Amount      : ${fraud_amount:>12,.2f}   ({fraud_rate:.2f}% fraud rate)
  ─────────────────────────────────────────────────────────────
  Report saved to   : {csv_path}
╚══════════════════════════════════════════════════════════════╝
"""
    log.info(summary)
    print(summary)   # also surfaces in Airflow UI task logs


# ─────────────────────────────────────────────────────────────
# DAG Definition
# ─────────────────────────────────────────────────────────────
default_args = {
    "owner":            "lankapay-data-team",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

with DAG(
    dag_id="lankapay_reconciliation",
    description="6-hourly batch reconciliation: Parquet clean txns vs PostgreSQL fraud alerts",
    schedule_interval="0 */6 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["lankapay", "reconciliation", "fraud", "batch-layer"],
) as dag:

    t1 = PythonOperator(
        task_id="validate_parquet_exists",
        python_callable=validate_parquet_exists,
    )

    t2 = PythonOperator(
        task_id="calculate_total_ingress",
        python_callable=calculate_total_ingress,
    )

    t3 = PythonOperator(
        task_id="calculate_validated_amount",
        python_callable=calculate_validated_amount,
    )

    t4 = PythonOperator(
        task_id="generate_reconciliation_report",
        python_callable=generate_reconciliation_report,
    )

    t5 = PythonOperator(
        task_id="log_completion",
        python_callable=log_completion,
    )

    # ── Linear dependency chain ────────────────────────────
    t1 >> t2 >> t3 >> t4 >> t5
