"""
============================================================
 LankaPay Wallet — Phase 5 Analytic Report
 File: reports/generate_analytic_report.py

 Queries PostgreSQL fraud_alerts table and produces:
   1. /reports/fraud_by_merchant.csv   — raw data
   2. /reports/fraud_by_merchant.png   — bar chart
   3. /reports/fraud_summary.txt       — printed summary

 Run inside the Spark or a plain Python container:
   docker exec lankapay-spark-master \
     python /opt/spark_jobs/generate_analytic_report.py

 Or on your host (with pip install psycopg2-binary matplotlib pandas):
   python reports/generate_analytic_report.py --local
============================================================
"""

import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

# ── Config ────────────────────────────────────────────────────
REPORTS_DIR = Path("/reports")

# Connection used inside Docker network
DOCKER_CONN = {
    "host":     "postgres",
    "port":     5432,
    "dbname":   "lankapay_db",
    "user":     "lankapay",
    "password": "lankapay123",
}

# Connection used from host machine (--local flag)
LOCAL_CONN = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "lankapay_db",
    "user":     "lankapay",
    "password": "lankapay123",
}


# ─────────────────────────────────────────────────────────────
# 1. Query PostgreSQL
# ─────────────────────────────────────────────────────────────
def fetch_fraud_data(conn_params: Dict) -> List[Dict]:
    """
    Returns rows of:
      merchant_category | fraud_count | total_fraud_amount | avg_fraud_amount
      high_value_count  | travel_count
    """
    import psycopg2
    import psycopg2.extras

    query = """
        SELECT
            merchant_category,
            COUNT(*)                                        AS fraud_count,
            ROUND(SUM(amount)::NUMERIC, 2)                 AS total_fraud_amount,
            ROUND(AVG(amount)::NUMERIC, 2)                 AS avg_fraud_amount,
            COUNT(*) FILTER (WHERE fraud_type = 'HIGH_VALUE')          AS high_value_count,
            COUNT(*) FILTER (WHERE fraud_type = 'IMPOSSIBLE_TRAVEL')   AS travel_count
        FROM   fraud_alerts
        GROUP  BY merchant_category
        ORDER  BY fraud_count DESC;
    """

    conn = psycopg2.connect(**conn_params)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(query)
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()

    if not rows:
        print("[WARN] No rows returned from fraud_alerts — is Spark running?")

    return rows


# ─────────────────────────────────────────────────────────────
# 2. Write CSV
# ─────────────────────────────────────────────────────────────
def write_csv(rows: List[Dict], path: Path) -> None:
    if not rows:
        return

    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[CSV]  Written → {path}")


# ─────────────────────────────────────────────────────────────
# 3. Generate bar chart
# ─────────────────────────────────────────────────────────────
def generate_chart(rows: List[Dict], path: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")          # non-interactive backend — safe in containers
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import matplotlib.ticker as mticker
    import numpy as np

    if not rows:
        print("[WARN] No data to chart.")
        return

    categories   = [r["merchant_category"].replace("_", "\n") for r in rows]
    fraud_counts = [int(r["fraud_count"])         for r in rows]
    hv_counts    = [int(r["high_value_count"])     for r in rows]
    travel_counts= [int(r["travel_count"])         for r in rows]
    totals       = [float(r["total_fraud_amount"]) for r in rows]

    x      = np.arange(len(categories))
    width  = 0.55

    # ── Colour palette ────────────────────────────────────────
    COLOR_BG       = "#E6EDF3"
    COLOR_GRID     = "#21262D"
    COLOR_HV       = "#E05C4B"   # high-value — red-orange
    COLOR_TRAVEL   = "#F0A500"   # impossible travel — amber
    COLOR_TEXT     = "#0D1117"
    COLOR_SUBTEXT  = "#8B949E"
    COLOR_ACCENT   = "#58A6FF"

    fig, axes = plt.subplots(
        1, 2,
        figsize=(16, 7),
        facecolor=COLOR_BG,
        gridspec_kw={"width_ratios": [2, 1]},
    )
    fig.subplots_adjust(wspace=0.35, left=0.07, right=0.96, top=0.82, bottom=0.14)

    # ── LEFT: Stacked bar — fraud count by type ───────────────
    ax = axes[0]
    ax.set_facecolor(COLOR_BG)

    bars_hv     = ax.bar(x, hv_counts,     width, label="High Value",         color=COLOR_HV,     zorder=3)
    bars_travel = ax.bar(x, travel_counts, width, label="Impossible Travel",   color=COLOR_TRAVEL, bottom=hv_counts, zorder=3)

    # Total label on top of each bar
    for i, total in enumerate(fraud_counts):
        ax.text(
            x[i], total + 0.4,
            str(total),
            ha="center", va="bottom",
            color=COLOR_TEXT, fontsize=9, fontweight="bold",
        )

    ax.set_xticks(x)
    ax.set_xticklabels(categories, color=COLOR_SUBTEXT, fontsize=8.5)
    ax.set_ylabel("Fraud Incident Count", color=COLOR_SUBTEXT, fontsize=10)
    ax.set_title("Fraud Incidents by Merchant Category", color=COLOR_TEXT, fontsize=13, fontweight="bold", pad=10)
    ax.tick_params(colors=COLOR_SUBTEXT)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))

    for spine in ax.spines.values():
        spine.set_edgecolor(COLOR_GRID)
    ax.set_axisbelow(True)
    ax.yaxis.grid(True, color=COLOR_GRID, linewidth=0.8, zorder=0)
    ax.xaxis.grid(False)

    legend = ax.legend(
        handles=[
            mpatches.Patch(color=COLOR_HV,     label="High Value (>$5k)"),
            mpatches.Patch(color=COLOR_TRAVEL, label="Impossible Travel"),
        ],
        facecolor="#161B22", edgecolor=COLOR_GRID,
        labelcolor=COLOR_TEXT, fontsize=9,
        loc="upper right",
    )

    # ── RIGHT: Horizontal bar — total fraud amount ($) ────────
    ax2 = axes[1]
    ax2.set_facecolor(COLOR_BG)

    # Gradient-like effect with alpha variation
    bar_colors = [COLOR_ACCENT] * len(categories)
    hbars = ax2.barh(x, totals, height=width, color=bar_colors, zorder=3, alpha=0.85)

    # Amount labels inside/outside bars
    max_total = max(totals) if totals else 1
    for i, val in enumerate(totals):
        label_x = val + max_total * 0.02
        ax2.text(
            label_x, x[i],
            f"${val:,.0f}",
            va="center", color=COLOR_TEXT, fontsize=8,
        )

    ax2.set_yticks(x)
    ax2.set_yticklabels(categories, color=COLOR_SUBTEXT, fontsize=8.5)
    ax2.set_xlabel("Total Fraud Amount (USD)", color=COLOR_SUBTEXT, fontsize=10)
    ax2.set_title("Total Fraud Amount ($)", color=COLOR_TEXT, fontsize=13, fontweight="bold", pad=10)
    ax2.tick_params(colors=COLOR_SUBTEXT)
    ax2.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"${v:,.0f}"))

    for spine in ax2.spines.values():
        spine.set_edgecolor(COLOR_GRID)
    ax2.set_axisbelow(True)
    ax2.xaxis.grid(True, color=COLOR_GRID, linewidth=0.8, zorder=0)
    ax2.yaxis.grid(False)
    ax2.invert_yaxis()

    # ── Figure-level header ───────────────────────────────────
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    fig.text(
        0.5, 0.95,
        "LankaPay Wallet — Fraud Detection Analytics",
        ha="center", va="top",
        color=COLOR_TEXT, fontsize=15, fontweight="bold",
    )
    fig.text(
        0.5, 0.905,
        f"Source: fraud_alerts table  ·  Generated: {generated_at}",
        ha="center", va="top",
        color=COLOR_SUBTEXT, fontsize=9,
    )

    # Thin accent line under the header
    fig.add_artist(
        plt.Line2D(
            [0.07, 0.96], [0.895, 0.895],
            transform=fig.transFigure,
            color=COLOR_GRID, linewidth=0.8,
        )
    )

    plt.savefig(path, dpi=150, bbox_inches="tight", facecolor=COLOR_BG)
    plt.close()
    print(f"[PNG]  Written → {path}")


# ─────────────────────────────────────────────────────────────
# 4. Print summary to stdout
# ─────────────────────────────────────────────────────────────
def print_summary(rows: List[Dict]) -> None:
    if not rows:
        return

    total_incidents = sum(int(r["fraud_count"])         for r in rows)
    total_amount    = sum(float(r["total_fraud_amount"]) for r in rows)
    top             = rows[0]

    summary = f"""
╔══════════════════════════════════════════════════════════════╗
  LankaPay — Fraud Attempts by Merchant Category
  ─────────────────────────────────────────────────────────────
  Total fraud incidents : {total_incidents:,}
  Total fraud amount    : ${total_amount:,.2f}
  ─────────────────────────────────────────────────────────────
  {"Category":<22} {"Count":>6}  {"Amount":>12}  {"Avg":>10}
  {"─"*22}  {"─"*6}  {"─"*12}  {"─"*10}"""

    for r in rows:
        summary += (
            f"\n  {r['merchant_category']:<22} "
            f"{int(r['fraud_count']):>6}  "
            f"${float(r['total_fraud_amount']):>11,.2f}  "
            f"${float(r['avg_fraud_amount']):>9,.2f}"
        )

    summary += f"""
  ─────────────────────────────────────────────────────────────
  Highest risk category : {top['merchant_category']}  ({top['fraud_count']} incidents)
╚══════════════════════════════════════════════════════════════╝"""

    print(summary)

    # Also write to file
    txt_path = REPORTS_DIR / "fraud_summary.txt"
    with open(txt_path, "w") as f:
        f.write(summary)
    print(f"[TXT]  Written → {txt_path}")


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="LankaPay Phase 5 Analytic Report")
    parser.add_argument(
        "--local", action="store_true",
        help="Connect to localhost:5432 instead of postgres:5432 (run from host)"
    )
    args = parser.parse_args()

    conn_params = LOCAL_CONN if args.local else DOCKER_CONN

    print("=" * 65)
    print("  LankaPay — Generating Fraud Analytic Report (Phase 5)")
    print(f"  DB: {conn_params['host']}:{conn_params['port']}/{conn_params['dbname']}")
    print("=" * 65)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # ── Install dependencies if missing (container-friendly) ──
    try:
        import psycopg2
        import matplotlib
        import pandas
        import numpy
    except ImportError:
        import subprocess
        print("[INFO] Installing dependencies...")
        subprocess.check_call([
            sys.executable, "-m", "pip", "install",
            "psycopg2-binary", "matplotlib", "pandas", "numpy",
            "--quiet"
        ])

    rows = fetch_fraud_data(conn_params)

    if not rows:
        print("[ERROR] No fraud data found. Make sure the producer and Spark job have been running.")
        sys.exit(1)

    csv_path = REPORTS_DIR / "fraud_by_merchant.csv"
    png_path = REPORTS_DIR / "fraud_by_merchant.png"

    write_csv(rows, csv_path)
    generate_chart(rows, png_path)
    print_summary(rows)

    print()
    print("  ✓ Phase 5 complete. Files in /reports/:")
    print(f"    - fraud_by_merchant.csv")
    print(f"    - fraud_by_merchant.png")
    print(f"    - fraud_summary.txt")


if __name__ == "__main__":
    main()
