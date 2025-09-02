from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pathlib

# ==== CONFIG ====
CONN_ID = "postgres_default"     # Airflow Postgres connection
CSV_FILENAME = "online_retail_II.csv"
TABLE = "public.online_retail"
CSV_PATH = pathlib.Path("/usr/local/airflow") / "data" / CSV_FILENAME
# =================

# Python function to load CSV
def load_csv():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    hook = PostgresHook(CONN_ID)
    conn = hook.get_conn()

    copy_sql = f"""
        COPY {TABLE} (
            invoice,
            stockcode,
            description,
            quantity,
            invoicedate,
            price,
            customer_id,
            country
        )
        FROM STDIN WITH (FORMAT CSV, HEADER TRUE);
    """

    with conn, conn.cursor() as cur, open(CSV_PATH, "r", encoding="utf-8") as f:
        cur.copy_expert(copy_sql, f)


# === DAG definition ===
with DAG(
    dag_id="online_retail_pipeline",
    start_date=datetime(2025, 9, 1),
    schedule=None,   # run manually
    catchup=False,
    tags=["postgres", "csv", "retail"],
) as dag:

    # 1️⃣ Create table
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id=CONN_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            invoice      INT,
            stockcode    TEXT,
            description  TEXT,
            quantity     INT,
            invoicedate  TIMESTAMP,
            price        NUMERIC(10,2),
            customer_id  INT,
            country      TEXT
        );
        """
    )

    # 2️⃣ Load CSV data
    load_data = PythonOperator(
        task_id="load_csv",
        python_callable=load_csv,
    )

    # Task dependencies → First create table, then load data
    create_table >> load_data
