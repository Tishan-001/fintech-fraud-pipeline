"""
============================================================
 LankaPay Wallet — Kafka Transaction Producer
 File: producers/transaction_producer.py

 Simulates real-time wallet transactions.
 - 95% normal transactions
 -  5% fraud injections (high-value OR impossible travel)

 Usage:
   pip install kafka-python faker
   python transaction_producer.py
============================================================
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

# ── Config ────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC            = "transactions"
SLEEP_BETWEEN_MESSAGES = 0.5   # seconds — simulates real-time flow

# Merchant categories LankaPay supports
MERCHANT_CATEGORIES = [
    "groceries", "electronics", "clothing", "restaurants",
    "bills", "travel", "entertainment", "healthcare", "online_shopping"
]

# Local country codes (normal transactions)
LOCAL_COUNTRIES = ["LK"]

# Foreign country codes used for impossible-travel fraud
FOREIGN_COUNTRIES = ["RU", "NG", "CN", "BR", "UA", "KP", "IR", "VN"]

# ── Track recent transactions per user for impossible-travel ──
# { user_id: {"timestamp": datetime, "location": str} }
recent_user_txn: dict = {}

# ── Helpers ───────────────────────────────────────────────────

def generate_user_id() -> str:
    """Pick a user from a small pool so impossible-travel triggers realistically."""
    return f"U{random.randint(1001, 1020):04d}"


def make_normal_transaction(user_id: str) -> dict:
    return {
        "txn_id":            str(uuid.uuid4()),
        "user_id":           user_id,
        "timestamp":         datetime.now(timezone.utc).isoformat(),
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "amount":            round(random.uniform(5.0, 499.0), 2),
        "location":          "LK",
    }


def make_high_value_fraud(user_id: str) -> dict:
    """Amount > $5000 — triggers Rule 1 in Spark."""
    return {
        "txn_id":            str(uuid.uuid4()),
        "user_id":           user_id,
        "timestamp":         datetime.now(timezone.utc).isoformat(),
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "amount":            round(random.uniform(5001.0, 15000.0), 2),
        "location":          random.choice(FOREIGN_COUNTRIES),
    }


def make_impossible_travel_fraud(user_id: str) -> list[dict]:
    """
    Two transactions from the same user within seconds but different countries.
    Returns a LIST of two transactions — both are published to Kafka.
    Triggers Rule 2 in Spark (two different locations within 10-min window).
    """
    ts_now = datetime.now(timezone.utc).isoformat()
    foreign = random.choice(FOREIGN_COUNTRIES)

    txn1 = {
        "txn_id":            str(uuid.uuid4()),
        "user_id":           user_id,
        "timestamp":         ts_now,
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "amount":            round(random.uniform(50.0, 800.0), 2),
        "location":          "LK",
    }
    txn2 = {
        "txn_id":            str(uuid.uuid4()),
        "user_id":           user_id,
        "timestamp":         ts_now,          # same timestamp window
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "amount":            round(random.uniform(50.0, 800.0), 2),
        "location":          foreign,
    }
    return [txn1, txn2]


def log_normal(txn: dict):
    print(
        f"  [NORMAL]        {txn['user_id']} | "
        f"${txn['amount']:>8.2f} | "
        f"{txn['location']:<4} | "
        f"{txn['merchant_category']}"
    )


def log_fraud(txn: dict, fraud_type: str):
    print(
        f"  [FRAUD_INJECT]  {txn['user_id']} | "
        f"${txn['amount']:>8.2f} | "
        f"{txn['location']:<4} | "
        f"{txn['merchant_category']:<18} | ⚠  {fraud_type}"
    )


# ── Main ──────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("  LankaPay Wallet — Transaction Producer")
    print(f"  Kafka: {KAFKA_BOOTSTRAP}  →  Topic: {TOPIC}")
    print("  Press Ctrl+C to stop.")
    print("=" * 65)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # Partition by user_id so all txns from same user go to same partition.
        # This is critical for Spark's stateful impossible-travel detection.
        key_serializer=lambda k: k.encode("utf-8"),
    )

    total_sent  = 0
    fraud_count = 0

    try:
        while True:
            user_id = generate_user_id()

            # ── Decide: normal (95%) or fraud (5%) ───────────
            roll = random.random()

            if roll < 0.95:
                # ── Normal transaction ─────────────────────
                txn = make_normal_transaction(user_id)
                producer.send(TOPIC, key=user_id, value=txn)
                log_normal(txn)
                total_sent += 1

            elif roll < 0.975:
                # ── Fraud type 1: High-value spike (~2.5%) ─
                txn = make_high_value_fraud(user_id)
                producer.send(TOPIC, key=user_id, value=txn)
                log_fraud(txn, "HIGH_VALUE (amount > $5000)")
                total_sent += 1
                fraud_count += 1

            else:
                # ── Fraud type 2: Impossible travel (~2.5%) ─
                pair = make_impossible_travel_fraud(user_id)
                for txn in pair:
                    producer.send(TOPIC, key=user_id, value=txn)
                log_fraud(pair[0], f"IMPOSSIBLE_TRAVEL → LK then {pair[1]['location']}")
                log_fraud(pair[1], f"IMPOSSIBLE_TRAVEL → LK then {pair[1]['location']}")
                total_sent  += 2
                fraud_count += 2

            # ── Progress summary every 20 messages ───────────
            if total_sent % 20 == 0:
                rate = (fraud_count / total_sent) * 100
                print(f"\n  ── Stats: {total_sent} sent | "
                      f"{fraud_count} fraud injected | "
                      f"fraud rate: {rate:.1f}% ──\n")

            time.sleep(SLEEP_BETWEEN_MESSAGES)

    except KeyboardInterrupt:
        print(f"\n\n  Producer stopped. Total sent: {total_sent} | Fraud: {fraud_count}")

    finally:
        producer.flush()
        producer.close()
        print("  Kafka producer closed cleanly.")


if __name__ == "__main__":
    main()
