from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="create_table",
    start_date=datetime(2025, 9, 1),
    schedule=None,      # run manually
    catchup=False,
) as dag:

   drop_table = PostgresOperator(
    task_id="create_table_task",
    postgres_conn_id="postgres_default",   # make sure this connection exists
    sql="""
    CREATE TABLE IF NOT EXISTS public.online_retail (
    invoice      INT,
    stockcode    TEXT,
    description  TEXT,
    quantity     INT,
    invoicedate  TIMESTAMP,
    price        NUMERIC(10,2),
    customer_id  INT,
    country      TEXT
);"""
)
