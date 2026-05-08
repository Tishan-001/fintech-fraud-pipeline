#!/bin/bash
# ============================================================
#  pg-init.sh — runs ONCE on first container start
#  Creates the `airflow` DB before init_db.sql executes
# ============================================================
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE airflow'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
    GRANT ALL PRIVILEGES ON DATABASE airflow TO $POSTGRES_USER;
EOSQL

echo "[pg-init] airflow database ready."
