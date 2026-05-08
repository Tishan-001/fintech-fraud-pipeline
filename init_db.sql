-- ============================================================
--  LankaPay Wallet — Database Initialisation
--  Runs automatically when PostgreSQL container first starts
--  File: 01-init_db.sql  (runs AFTER 00-pg-init.sh)
-- ============================================================

-- Switch to the LankaPay app database
\c lankapay_db;

-- ── Fraud alerts table (written by Spark Streaming) ─────────
CREATE TABLE IF NOT EXISTS fraud_alerts (
    id                SERIAL PRIMARY KEY,
    user_id           VARCHAR(20)    NOT NULL,
    timestamp         TIMESTAMPTZ    NOT NULL,
    merchant_category VARCHAR(50),
    amount            NUMERIC(12, 2) NOT NULL,
    location          VARCHAR(10),
    fraud_type        VARCHAR(50)    NOT NULL,
    detected_at       TIMESTAMPTZ    DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fraud_merchant  ON fraud_alerts (merchant_category);
CREATE INDEX IF NOT EXISTS idx_fraud_timestamp ON fraud_alerts (timestamp);
CREATE INDEX IF NOT EXISTS idx_fraud_user      ON fraud_alerts (user_id);

-- ── Reconciliation log (written by Airflow DAG) ─────────────
CREATE TABLE IF NOT EXISTS reconciliation_log (
    id                   SERIAL PRIMARY KEY,
    period_start         TIMESTAMPTZ   NOT NULL,
    period_end           TIMESTAMPTZ   NOT NULL,
    total_ingress_amount NUMERIC(18,2),
    validated_amount     NUMERIC(18,2),
    fraud_amount         NUMERIC(18,2),
    fraud_rate_pct       NUMERIC(6,2),
    created_at           TIMESTAMPTZ   DEFAULT NOW()
);

\echo '>>> LankaPay tables created successfully.'
