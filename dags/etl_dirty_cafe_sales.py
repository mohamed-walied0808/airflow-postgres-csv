from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import numpy as np
import os

# =====================
# CONFIG
# =====================
RAW_PATH = "/usr/local/airflow/data/dirty_cafe_sales.csv"
CLEAN_PATH = "/usr/local/airflow/data/cleaned_cafe_sales.csv"
CONN_ID = "postgres_default"
TABLE = "public.cafe_sales_clean"

# Menu reference (for fixing prices)
MENU = {
    "Coffee": 2.0, "Tea": 1.5, "Sandwich": 4.0,
    "Salad": 5.0, "Cake": 3.0, "Cookie": 1.0,
    "Smoothie": 4.0, "Juice": 3.0
}

# =====================
# EXTRACT + TRANSFORM
# =====================
def extract_transform(input_path, output_path):
    # Load raw data
    df = pd.read_csv(input_path)

    # Replace invalid markers with NaN
    df.replace(["ERROR", "UNKNOWN", ""], np.nan, inplace=True)

    # Convert numeric columns
    for col in ["Quantity", "Price Per Unit", "Total Spent"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Convert dates
    df["Transaction Date"] = pd.to_datetime(df["Transaction Date"], errors="coerce")

    # Fill missing values
    for col in ["Quantity", "Price Per Unit", "Total Spent"]:
        df[col].fillna(df[col].median(), inplace=True)

    for col in ["Item", "Payment Method", "Location"]:
        df[col].fillna("Unknown", inplace=True)

    # Fix prices using menu
    df["Price Per Unit"] = df.apply(
        lambda row: MENU.get(row["Item"], row["Price Per Unit"]),
        axis=1
    )

    # Drop invalid items not in menu
    df = df[df["Item"].isin(MENU.keys())]

    # Recalculate totals
    df["Total Spent"] = df["Quantity"] * df["Price Per Unit"]

    # Drop invalid/future dates
    df = df.dropna(subset=["Transaction Date"])
    df = df[df["Transaction Date"] <= pd.Timestamp.today()]

    # Save cleaned CSV
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)

    print(f"✅ Cleaned data saved to {output_path}, rows: {df.shape[0]}")

# =====================
# LOAD INTO POSTGRES
# =====================
def load_to_postgres(csv_path, table, conn_id):
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    with open(csv_path, "r", encoding="utf-8") as f:
        cursor.copy_expert(
            f"""
            COPY {table} (
                "Transaction ID", Item, Quantity, "Price Per Unit",
                "Total Spent", "Payment Method", Location, "Transaction Date"
            )
            FROM STDIN WITH CSV HEADER
            DELIMITER ',' QUOTE '"';
            """,
            f,
        )
    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Data loaded into {table}")

# =====================
# DAG DEFINITION
# =====================
with DAG(
    dag_id="cafe_sales_etl",
    start_date=datetime(2025, 9, 1),
    schedule=None,   # run manually
    catchup=False,
    tags=["etl", "postgres", "csv", "cafe"],
) as dag:

    # 1️⃣ Create table if not exists
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id=CONN_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            "Transaction ID" TEXT,
            Item TEXT,
            Quantity NUMERIC(10,2),
            "Price Per Unit" NUMERIC(10,2),
            "Total Spent" NUMERIC(10,2),
            "Payment Method" TEXT,
            Location TEXT,
            "Transaction Date" DATE
        );
        """
    )

    # 2️⃣ Extract + Transform
    transform = PythonOperator(
        task_id="extract_transform",
        python_callable=extract_transform,
        op_kwargs={"input_path": RAW_PATH, "output_path": CLEAN_PATH},
    )

    # 3️⃣ Load into Postgres
    load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
        op_kwargs={"csv_path": CLEAN_PATH, "table": TABLE, "conn_id": CONN_ID},
    )

    # Pipeline order
    create_table >> transform >> load
