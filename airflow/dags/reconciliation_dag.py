"""
============================================================
 LankaPay Wallet — Airflow Reconciliation DAG
 File: airflow/dags/reconciliation_dag.py

 Schedule: every 6 hours  →  0 */6 * * *

 DAG task order:
   t0: check_and_alert_fraud         
         ↓
   t1: validate_parquet_exists
         ↓
   t2: calculate_total_ingress
         ↓
   t3: calculate_validated_amount
         ↓
   t4: generate_reconciliation_report
         ↓
   t5: log_completion
============================================================
"""

from __future__ import annotations

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

FRAUD_THRESHOLD = 50_000.0   # LKR
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────────────────────
def _get_window(logical_date: datetime):
    hour_block   = (logical_date.hour // 6) * 6
    period_start = logical_date.replace(
        hour=hour_block, minute=0, second=0, microsecond=0,
        tzinfo=timezone.utc
    )
    period_end = period_start + timedelta(hours=6)
    return period_start, period_end


# ─────────────────────────────────────────────────────────────
# t0 — check_and_alert_fraud
# ─────────────────────────────────────────────────────────────
def check_and_alert_fraud(**context) -> None:
    """
    Queries fraud_alerts for the current 6-hour window.
    Logs a loud, structured alert for every fraud record found.
    This surfaces directly in the Airflow task log and UI.

    Think of it as the DAG's built-in fraud bulletin —
    emitted at the top of every reconciliation run before
    any heavy Parquet/reporting work begins.
    """
    import psycopg2
    import psycopg2.extras

    ti                       = context["ti"]
    logical_date             = context["logical_date"]
    period_start, period_end = _get_window(logical_date)

    log.info("Fraud alert check — window: %s -> %s", period_start, period_end)

    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT id, user_id, timestamp, merchant_category,
                   amount, location, fraud_type, detected_at
            FROM   fraud_alerts
            WHERE  timestamp >= %s AND timestamp < %s
            ORDER  BY amount DESC
            """,
            (period_start, period_end),
        )
        fraud_rows = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
    except Exception as exc:
        log.error("Could not query fraud_alerts: %s", exc)
        fraud_rows = []

    # ── No fraud in this window ───────────────────────────────
    if not fraud_rows:
        msg = (
            "\n"
            "================================================================\n"
            "  FRAUD CHECK -- ALL CLEAR\n"
            f"  Window : {period_start.strftime('%Y-%m-%d %H:%M')} -> "
            f"{period_end.strftime('%H:%M')} UTC\n"
            "  No fraud alerts found in this period.\n"
            "================================================================"
        )
        log.info(msg)
        print(msg)
        ti.xcom_push(key="fraud_alert_count",        value=0)
        ti.xcom_push(key="fraud_alert_total_amount", value=0.0)
        return

    # ── Fraud found — fire individual alerts ─────────────────
    total_fraud_amount = sum(float(r["amount"]) for r in fraud_rows)
    hv_count     = sum(1 for r in fraud_rows if r["fraud_type"] == "HIGH_VALUE")
    travel_count = sum(1 for r in fraud_rows if r["fraud_type"] == "IMPOSSIBLE_TRAVEL")

    header = (
        "\n"
        "================================================================\n"
        f"  *** FRAUD ALERT BULLETIN — {len(fraud_rows)} INCIDENT(S) DETECTED ***\n"
        f"  Window             : {period_start.strftime('%Y-%m-%d %H:%M')} -> "
        f"{period_end.strftime('%H:%M')} UTC\n"
        f"  Total Fraud Amount : LKR {total_fraud_amount:,.2f}\n"
        f"  High-Value (>LKR 50,000) : {hv_count} incident(s)\n"
        f"  Impossible Travel  : {travel_count} incident(s)\n"
        "================================================================"
    )
    log.warning(header)
    print(header)

    # Per-fraud-record detail
    for i, row in enumerate(fraud_rows, 1):
        if row["fraud_type"] == "HIGH_VALUE":
            label = "[HIGH-VALUE > LKR 50,000]"
        else:
            label = "[IMPOSSIBLE TRAVEL]"

        detail = (
            f"\n  Alert {i}/{len(fraud_rows)}  {label}\n"
            f"  ----------------------------------------------------------------\n"
            f"  Alert ID      : {row['id']}\n"
            f"  User          : {row['user_id']}\n"
            f"  Amount        : LKR {float(row['amount']):>14,.2f}\n"
            f"  Location      : {row['location']}\n"
            f"  Merchant Cat. : {row['merchant_category']}\n"
            f"  Event Time    : {row['timestamp']}\n"
            f"  Detected At   : {row['detected_at']}\n"
        )
        log.warning(detail)
        print(detail)

    footer = "================================================================\n"
    log.warning(footer)
    print(footer)

    ti.xcom_push(key="fraud_alert_count",        value=len(fraud_rows))
    ti.xcom_push(key="fraud_alert_total_amount", value=total_fraud_amount)


# ─────────────────────────────────────────────────────────────
# t1 — validate_parquet_exists
# ─────────────────────────────────────────────────────────────
def validate_parquet_exists(**context) -> None:
    logical_date             = context["logical_date"]
    period_start, period_end = _get_window(logical_date)

    log.info("Reconciliation window: %s -> %s", period_start, period_end)

    if not PARQUET_DIR.exists():
        raise AirflowSkipException(
            f"Parquet root {PARQUET_DIR} does not exist — Spark may not have started yet."
        )

    partition_dates = set()
    for item in PARQUET_DIR.iterdir():
        if item.is_dir() and item.name.startswith("date="):
            partition_dates.add(item.name)

    if not partition_dates:
        raise AirflowSkipException("No Parquet partitions found — skipping reconciliation.")

    log.info("Found partitions: %s", sorted(partition_dates))

    context["ti"].xcom_push(key="period_start", value=period_start.isoformat())
    context["ti"].xcom_push(key="period_end",   value=period_end.isoformat())
    context["ti"].xcom_push(key="partitions",   value=sorted(partition_dates))


# ─────────────────────────────────────────────────────────────
# t2 — calculate_total_ingress
# ─────────────────────────────────────────────────────────────
def calculate_total_ingress(**context) -> None:
    import pandas as pd
    import psycopg2

    ti           = context["ti"]
    period_start = datetime.fromisoformat(ti.xcom_pull(key="period_start", task_ids="validate_parquet_exists"))
    period_end   = datetime.fromisoformat(ti.xcom_pull(key="period_end",   task_ids="validate_parquet_exists"))

    parquet_dfs = []
    for partition in PARQUET_DIR.glob("date=*"):
        if partition.is_dir():
            try:
                parquet_dfs.append(pd.read_parquet(partition))
            except Exception as exc:
                log.warning("Could not read partition %s: %s", partition, exc)

    if parquet_dfs:
        all_clean              = pd.concat(parquet_dfs, ignore_index=True)
        all_clean["timestamp"] = pd.to_datetime(all_clean["timestamp"], utc=True)
        mask                   = (all_clean["timestamp"] >= period_start) & \
                                 (all_clean["timestamp"] <  period_end)
        window_clean           = all_clean[mask]
        clean_amount           = float(window_clean["amount"].sum())
        clean_count            = int(len(window_clean))
    else:
        clean_amount = 0.0
        clean_count  = 0

    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        cur  = conn.cursor()
        cur.execute(
            "SELECT COALESCE(SUM(amount), 0), COUNT(*) FROM fraud_alerts "
            "WHERE timestamp >= %s AND timestamp < %s",
            (period_start, period_end),
        )
        fraud_amount, fraud_count = cur.fetchone()
        fraud_amount = float(fraud_amount)
        fraud_count  = int(fraud_count)
        cur.close()
        conn.close()
    except Exception as exc:
        log.warning("Could not query fraud_alerts: %s", exc)
        fraud_amount = 0.0
        fraud_count  = 0

    total_ingress = clean_amount + fraud_amount
    total_count   = clean_count + fraud_count

    log.info(
        "Total ingress: LKR %.2f (%d txns) | Clean: LKR %.2f (%d) | Fraud: LKR %.2f (%d)",
        total_ingress, total_count, clean_amount, clean_count, fraud_amount, fraud_count,
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
        df              = pd.concat(parquet_dfs, ignore_index=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        mask            = (df["timestamp"] >= period_start) & (df["timestamp"] < period_end)
        df              = df[mask]
        df              = df[df["amount"] > 0]
        df              = df[df["user_id"].notna() & (df["user_id"] != "")]
        df              = df[df["merchant_category"].notna() & (df["merchant_category"] != "")]
        validated_amount = float(df["amount"].sum())
        validated_count  = int(len(df))

    log.info("Validated amount: LKR %.2f (%d txns)", validated_amount, validated_count)
    ti.xcom_push(key="validated_amount", value=validated_amount)
    ti.xcom_push(key="validated_count",  value=validated_count)


# ─────────────────────────────────────────────────────────────
# t4 — generate_reconciliation_report
# ─────────────────────────────────────────────────────────────
def generate_reconciliation_report(**context) -> None:
    import psycopg2

    ti = context["ti"]

    period_start     = datetime.fromisoformat(ti.xcom_pull(key="period_start",        task_ids="validate_parquet_exists"))
    period_end       = datetime.fromisoformat(ti.xcom_pull(key="period_end",          task_ids="validate_parquet_exists"))
    total_ingress    = ti.xcom_pull(key="total_ingress_amount", task_ids="calculate_total_ingress")
    fraud_amount     = ti.xcom_pull(key="fraud_amount",         task_ids="calculate_total_ingress")
    clean_count      = ti.xcom_pull(key="clean_count",          task_ids="calculate_total_ingress")
    fraud_count      = ti.xcom_pull(key="fraud_count",          task_ids="calculate_total_ingress")
    validated_amount = ti.xcom_pull(key="validated_amount",     task_ids="calculate_validated_amount")

    fraud_rate_pct   = round((fraud_amount / total_ingress * 100), 4) if total_ingress > 0 else 0.0
    data_quality_gap = round(total_ingress - fraud_amount - validated_amount, 2)
    period_label     = f"{period_start.strftime('%Y-%m-%d %H:%M')}-{period_end.strftime('%H:%M')} UTC"
    generated_at     = datetime.now(timezone.utc).isoformat()

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

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"reconciliation_{period_start.strftime('%Y%m%d_%H%M')}.csv"
    csv_path = REPORTS_DIR / filename

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(report_row.keys()))
        writer.writeheader()
        writer.writerow(report_row)
    log.info("CSV written: %s", csv_path)

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
            (period_start, period_end,
             report_row["total_ingress_amount"], report_row["validated_amount"],
             report_row["fraud_amount"],         report_row["fraud_rate_pct"]),
        )
        conn.commit()
        cur.close()
        conn.close()
        log.info("Inserted reconciliation row into PostgreSQL.")
    except Exception as exc:
        log.error("Failed to insert into reconciliation_log: %s", exc)

    ti.xcom_push(key="csv_path", value=str(csv_path))


# ─────────────────────────────────────────────────────────────
# t5 — log_completion
# ─────────────────────────────────────────────────────────────
def log_completion(**context) -> None:
    ti = context["ti"]

    period_start       = ti.xcom_pull(key="period_start",             task_ids="validate_parquet_exists")
    period_end         = ti.xcom_pull(key="period_end",               task_ids="validate_parquet_exists")
    total_ingress      = ti.xcom_pull(key="total_ingress_amount",     task_ids="calculate_total_ingress")
    fraud_amount       = ti.xcom_pull(key="fraud_amount",             task_ids="calculate_total_ingress")
    validated_amount   = ti.xcom_pull(key="validated_amount",         task_ids="calculate_validated_amount")
    csv_path           = ti.xcom_pull(key="csv_path",                 task_ids="generate_reconciliation_report")
    alert_count        = ti.xcom_pull(key="fraud_alert_count",        task_ids="check_and_alert_fraud") or 0
    alert_total_amount = ti.xcom_pull(key="fraud_alert_total_amount", task_ids="check_and_alert_fraud") or 0.0

    fraud_rate = (fraud_amount / total_ingress * 100) if total_ingress else 0.0

    summary = f"""
================================================================
  LankaPay Reconciliation -- Run Complete
  Window  : {period_start}  ->  {period_end}
  ----------------------------------------------------------------
  Total Ingress     : LKR {total_ingress:>14,.2f}
  Validated Amount  : LKR {validated_amount:>14,.2f}
  Fraud Amount      : LKR {fraud_amount:>14,.2f}   ({fraud_rate:.2f}% fraud rate)
  ----------------------------------------------------------------
  Fraud Alerts Fired : {alert_count} incident(s)  /  LKR {alert_total_amount:,.2f} total
  ----------------------------------------------------------------
  Report saved to   : {csv_path}
================================================================
"""
    log.info(summary)
    print(summary)


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
    description="6-hourly batch reconciliation with fraud alert bulletin",
    schedule_interval="0 */6 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["lankapay", "reconciliation", "fraud", "batch-layer"],
) as dag:

    t0 = PythonOperator(
        task_id="check_and_alert_fraud",
        python_callable=check_and_alert_fraud,
    )

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

    # ── Task chain ─────────────────────────────────────────
    t0 >> t1 >> t2 >> t3 >> t4 >> t5