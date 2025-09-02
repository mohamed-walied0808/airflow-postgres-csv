from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os

# ---------------------------
# Function to check & COPY CSV into Postgres
# ---------------------------
def load_netflix_csv():
    file_path = "/usr/local/airflow/data/netflix_titles.csv"

    # ✅ Check the file first
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV file not found at {file_path}")

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    with open(file_path, "r", encoding="utf-8") as f:
        cursor.copy_expert(
            """
            COPY public.netflix_titles
            FROM STDIN
            WITH CSV HEADER
            DELIMITER ','
            QUOTE '"'
            """,
            f,
        )
    conn.commit()
    cursor.close()
    conn.close()


# ---------------------------
# Define DAG
# ---------------------------
with DAG(
    dag_id="load_netflix_csv_v2",
    start_date=datetime(2025, 9, 1),
    schedule=None,   # run manually for now
    catchup=False,
    tags=["postgres", "csv", "netflix"],
) as dag:

    # 1️⃣ Create the table
    create_table = PostgresOperator(
        task_id="create_netflix_table",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS public.netflix_titles (
            show_id TEXT,
            type TEXT,
            title TEXT,
            director TEXT,
            cast_members TEXT,   -- renamed 'cast' (reserved word)
            country TEXT,
            date_added TEXT,
            release_year INT,
            rating TEXT,
            duration TEXT,
            listed_in TEXT,
            description TEXT
        );
        """
    )

    # 2️⃣ Load the CSV
    load_csv = PythonOperator(
        task_id="load_csv_task",
        python_callable=load_netflix_csv
    )

    # Task order: create table → load data
    create_table >> load_csv
