"""
============================================================
 LankaPay Wallet — Spark Structured Streaming Fraud Detector
 File: spark_jobs/fraud_detector.py

 Reads from Kafka topic `transactions`, applies two fraud rules,
 writes results to:
   - PostgreSQL  → fraud_alerts table  (fraud transactions)
   - Parquet     → /data/warehouse/    (clean transactions)

 Fraud Rules:
   Rule 1 — High Value:       amount > LKR 50,000
   Rule 2 — Impossible Travel: same user_id, 2 different locations
                                within a 10-minute event-time window

 Run inside Spark container:
   docker exec lankapay-spark-master \
     /opt/spark/bin/spark-submit \
     --master spark://spark-master:7077 \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0 \
     /opt/spark_jobs/fraud_detector.py
============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window,
    collect_set, size, lit, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType
)

# ── Config ────────────────────────────────────────────────────
KAFKA_BROKER      = "kafka:29092"          # internal Docker network address
KAFKA_TOPIC       = "transactions"
CHECKPOINT_DIR    = "/tmp/checkpoints"

POSTGRES_URL      = "jdbc:postgresql://postgres:5432/lankapay_db"
POSTGRES_TABLE    = "fraud_alerts"
POSTGRES_PROPS    = {
    "user":     "lankapay",
    "password": "lankapay123",
    "driver":   "org.postgresql.Driver",
}

PARQUET_OUTPUT    = "/data/warehouse"
FRAUD_THRESHOLD   = 50000.0   # LKR
TRAVEL_WINDOW_MIN = "10 minutes"
WATERMARK_DELAY   = "10 minutes"

# ── Transaction JSON schema ───────────────────────────────────
TXN_SCHEMA = StructType([
    StructField("txn_id",            StringType(),  True),
    StructField("user_id",           StringType(),  True),
    StructField("timestamp",         StringType(),  True),   # parsed below
    StructField("merchant_category", StringType(),  True),
    StructField("amount",            DoubleType(),  True),
    StructField("location",          StringType(),  True),
])


# ── Write fraud alert batch to PostgreSQL ─────────────────────
def write_fraud_to_postgres(batch_df, batch_id):
    """
    Called by foreachBatch for each micro-batch of fraud records.
    Adds fraud_type column and detected_at timestamp before writing.
    """
    if batch_df.count() == 0:
        return

    batch_df \
        .withColumn("detected_at", current_timestamp()) \
        .select(
            "user_id", "timestamp", "merchant_category",
            "amount", "location", "fraud_type", "detected_at"
        ) \
        .write \
        .jdbc(
            url=POSTGRES_URL,
            table=POSTGRES_TABLE,
            mode="append",
            properties=POSTGRES_PROPS,
        )
    print(f"[Batch {batch_id}] Wrote {batch_df.count()} fraud alert(s) to PostgreSQL.")


# ── Write clean transactions to Parquet ──────────────────────
def write_clean_to_parquet(batch_df, batch_id):
    """
    Partitions clean transactions by date for efficient Airflow queries.
    Path: /data/warehouse/date=YYYY-MM-DD/
    """
    if batch_df.count() == 0:
        return

    from pyspark.sql.functions import to_date

    batch_df \
        .withColumn("date", to_date(col("timestamp"))) \
        .write \
        .partitionBy("date") \
        .mode("append") \
        .parquet(PARQUET_OUTPUT)
    print(f"[Batch {batch_id}] Wrote {batch_df.count()} clean transaction(s) to Parquet.")


# ── Main ──────────────────────────────────────────────────────
def main():
    spark = SparkSession.builder \
        .appName("LankaPay-FraudDetector") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("  LankaPay Fraud Detector — Spark Structured Streaming")
    print(f"  Kafka: {KAFKA_BROKER}  Topic: {KAFKA_TOPIC}")
    print("=" * 60)

    # ── 1. Read raw stream from Kafka ─────────────────────────
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # ── 2. Parse JSON payload ─────────────────────────────────
    parsed = raw_stream \
        .selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), TXN_SCHEMA).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .filter(col("user_id").isNotNull())     # drop malformed messages

    # Apply watermark using event time (the timestamp inside the JSON).
    # This is KEY for correct handling of late-arriving data —
    # Spark will wait up to WATERMARK_DELAY before closing a window.
    watermarked = parsed.withWatermark("timestamp", WATERMARK_DELAY)

    # ──────────────────────────────────────────────────────────
    # RULE 1 — High-Value Transaction (amount > LKR 50,000)
    # Simple filter, no windowing needed.
    # ──────────────────────────────────────────────────────────
    high_value_fraud = watermarked \
        .filter(col("amount") > FRAUD_THRESHOLD) \
        .withColumn("fraud_type", lit("HIGH_VALUE"))

    # ──────────────────────────────────────────────────────────
    # RULE 2 — Impossible Travel
    # Group by user_id in a 10-minute tumbling window.
    # If the same user_id has > 1 distinct location → fraud.
    # ──────────────────────────────────────────────────────────
    travel_fraud = watermarked \
        .groupBy(
            window(col("timestamp"), TRAVEL_WINDOW_MIN),
            col("user_id")
        ) \
        .agg(collect_set("location").alias("locations")) \
        .filter(size(col("locations")) > 1) \
        .withColumn("fraud_type",        lit("IMPOSSIBLE_TRAVEL")) \
        .withColumn("merchant_category", lit("MULTIPLE")) \
        .withColumn("amount",            lit(0.0)) \
        .withColumn("location",          col("locations").cast("string")) \
        .withColumn("timestamp",         col("window.start")) \
        .select(
            "user_id", "timestamp", "merchant_category",
            "amount", "location", "fraud_type"
        )

    # ──────────────────────────────────────────────────────────
    # CLEAN transactions — exclude high-value fraud (> LKR 50,000)
    # (impossible travel can't be excluded at row level here,
    #  Airflow reconciliation handles that distinction)
    # ──────────────────────────────────────────────────────────
    clean_txns = watermarked.filter(col("amount") <= FRAUD_THRESHOLD)

    # ── 3. Write streams ──────────────────────────────────────

    # Rule 1 fraud → PostgreSQL
    q1 = high_value_fraud \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(write_fraud_to_postgres) \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/high_value") \
        .trigger(processingTime="10 seconds") \
        .start()

    # Rule 2 fraud → PostgreSQL
    q2 = travel_fraud \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(write_fraud_to_postgres) \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/impossible_travel") \
        .trigger(processingTime="10 seconds") \
        .start()

    # Clean transactions → Parquet
    q3 = clean_txns \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(write_clean_to_parquet) \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/clean") \
        .trigger(processingTime="10 seconds") \
        .start()

    print("  3 streaming queries started. Waiting for fraud events...")
    print("  Q1: High-value → PostgreSQL")
    print("  Q2: Impossible travel → PostgreSQL")
    print("  Q3: Clean transactions → Parquet")
    print()

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
