"""
============================================================
 LankaPay Wallet — Real-Time Fraud Alert Watcher
 File: producers/fraud_alert_watcher.py

 Polls PostgreSQL fraud_alerts every 5 seconds.
 When a new HIGH_VALUE or IMPOSSIBLE_TRAVEL fraud is detected,
 fires a loud console alert with full transaction details.

 Run on your HOST machine (alongside the producer):
   pip install psycopg2-binary
   python producers/fraud_alert_watcher.py

 Or inside a container:
   docker exec lankapay-postgres python3 /fraud_alert_watcher.py
============================================================
"""

import time
import argparse
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────
POLL_INTERVAL_SEC = 5   # how often to check for new fraud alerts

LOCAL_CONN = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "lankapay_db",
    "user":     "lankapay",
    "password": "lankapay123",
}

DOCKER_CONN = {
    "host":     "postgres",
    "port":     5432,
    "dbname":   "lankapay_db",
    "user":     "lankapay",
    "password": "lankapay123",
}

# Alert colours (ANSI — works on Windows Terminal, macOS, Linux)
RED     = "\033[91m"
YELLOW  = "\033[93m"
GREEN   = "\033[92m"
CYAN    = "\033[96m"
BOLD    = "\033[1m"
RESET   = "\033[0m"
BLINK   = "\033[5m"


# ─────────────────────────────────────────────────────────────
# Alert renderer
# ─────────────────────────────────────────────────────────────
def render_alert(row: dict) -> None:
    fraud_type = row["fraud_type"]
    now        = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    if fraud_type == "HIGH_VALUE":
        colour = RED
        icon   = "💸"
        title  = "HIGH-VALUE TRANSACTION DETECTED"
    else:
        colour = YELLOW
        icon   = "✈️ "
        title  = "IMPOSSIBLE TRAVEL DETECTED"

    print()
    print(f"{colour}{BOLD}{'═' * 62}{RESET}")
    print(f"{colour}{BOLD}  {BLINK}⚠  FRAUD ALERT{RESET}{colour}{BOLD}  —  {title}  {icon}{RESET}")
    print(f"{colour}{BOLD}{'═' * 62}{RESET}")
    print(f"{colour}  Alert ID       : {RESET}{row['id']}")
    print(f"{colour}  User           : {RESET}{BOLD}{row['user_id']}{RESET}")
    print(f"{colour}  Fraud Type     : {RESET}{BOLD}{colour}{fraud_type}{RESET}")
    print(f"{colour}  Amount         : {RESET}{BOLD}LKR {float(row['amount']):,.2f}{RESET}")
    print(f"{colour}  Location       : {RESET}{row['location']}")
    print(f"{colour}  Merchant Cat.  : {RESET}{row['merchant_category']}")
    print(f"{colour}  Event Time     : {RESET}{row['timestamp']}")
    print(f"{colour}  Detected At    : {RESET}{row['detected_at']}")
    print(f"{colour}  Watcher Time   : {RESET}{now}")
    print(f"{colour}{BOLD}{'═' * 62}{RESET}")
    print()


# ─────────────────────────────────────────────────────────────
# Main polling loop
# ─────────────────────────────────────────────────────────────
def main(conn_params: dict) -> None:
    import psycopg2
    import psycopg2.extras

    print(f"{GREEN}{BOLD}")
    print("  ╔══════════════════════════════════════════════╗")
    print("  ║   LankaPay — Fraud Alert Watcher  🛡️         ║")
    print("  ║   Polling PostgreSQL every 5 seconds         ║")
    print("  ║   Press Ctrl+C to stop                       ║")
    print("  ╚══════════════════════════════════════════════╝")
    print(f"{RESET}")

    # Connect
    try:
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        print(f"{GREEN}  ✓ Connected to PostgreSQL at {conn_params['host']}:{conn_params['port']}{RESET}\n")
    except Exception as e:
        print(f"{RED}  ✗ Cannot connect to PostgreSQL: {e}{RESET}")
        print(f"  Make sure docker-compose is running and try again.")
        return

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Track the highest ID we have already alerted on
    cur.execute("SELECT COALESCE(MAX(id), 0) FROM fraud_alerts;")
    last_seen_id = cur.fetchone()["coalesce"]
    print(f"  Starting from fraud alert ID > {last_seen_id}  (existing alerts skipped)\n")

    alert_count = 0

    try:
        while True:
            # Fetch any new fraud alerts since last check
            cur.execute(
                """
                SELECT id, user_id, timestamp, merchant_category,
                       amount, location, fraud_type, detected_at
                FROM   fraud_alerts
                WHERE  id > %s
                ORDER  BY id ASC;
                """,
                (last_seen_id,),
            )
            new_rows = cur.fetchall()

            if new_rows:
                for row in new_rows:
                    render_alert(dict(row))
                    last_seen_id = max(last_seen_id, row["id"])
                    alert_count += 1
            else:
                # Heartbeat every 5 polls (25 seconds) so you know it's alive
                ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
                print(f"  {CYAN}[{ts}]{RESET}  Watching...  "
                      f"(alerts fired so far: {alert_count})", end="\r")

            time.sleep(POLL_INTERVAL_SEC)

    except KeyboardInterrupt:
        print(f"\n\n  Watcher stopped. Total alerts fired: {alert_count}")
    finally:
        cur.close()
        conn.close()
        print("  PostgreSQL connection closed cleanly.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LankaPay Fraud Alert Watcher")
    parser.add_argument(
        "--local", action="store_true",
        help="Connect to localhost:5432 (run from host). Default: postgres:5432 (inside Docker)"
    )
    args   = parser.parse_args()
    params = LOCAL_CONN if args.local else DOCKER_CONN
    main(params)
