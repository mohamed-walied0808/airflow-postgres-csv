from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pendulum
import pathlib

# ==== EDIT THESE ====
CONN_ID = "postgres_default"   # Airflow connection to your Postgres test DB
CSV_FILENAME = "online_retail_II.csv"  # put this under your project's data/ folder
TABLE = "public.online_retail"  # your existing table
# ====================

CSV_PATH = pathlib.Path("/usr/local/airflow") / "data" / CSV_FILENAME

def load_csv():
    # 1) Check file exists inside the container
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    # 2) Connect to Postgres using Airflow Connection
    hook = PostgresHook(CONN_ID)
    conn = hook.get_conn()

    # 3) COPY command (columns must match table + CSV header order)
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

    # 4) Stream the CSV into Postgres (server doesn’t need to see the file)
    with conn, conn.cursor() as cur, open(CSV_PATH, "r", encoding="utf-8") as f:
        cur.copy_expert(copy_sql, f)

with DAG(
    dag_id="load_online_retail_csv",
    start_date=datetime(2025, 9, 1),
    schedule=None,     # run manually
    catchup=False,
    tags=["csv","postgres","load"],
) as dag:

    load_task = PythonOperator(
        task_id="load_csv_task",
        python_callable=load_csv,
    )
