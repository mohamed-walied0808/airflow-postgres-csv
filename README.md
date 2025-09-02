🚀 Airflow Postgres EtL(EL) Mini Project

This repository contains Apache Airflow DAGs that demonstrate how to orchestrate data pipelines by creating tables in Postgres and loading CSV files into them.
The project simulates a simple (Extract, Load) workflow commonly used in real-world Data Engineering.

📂 Project Structure

airflow_project/
│── dags/
│   ├── create_load_netflix_data.py   # DAG to load Netflix data
│   ├── create_table_online_retail.py # DAG to load Online Retail data
│── data/
│   ├── netflix_titles.csv            # Netflix dataset (sample from Kaggle)
│   ├── online_retail_II.csv          # Online Retail dataset
│── Dockerfile
│── requirements.txt
│── README.md


DAGs Overview

1️⃣ online_retail_pipeline
Goal: Create online_retail table and load data from online_retail_II.csv.

Steps:
Create table with schema:
invoice      INT,
stockcode    TEXT,
description  TEXT,
quantity     INT,
invoicedate  TIMESTAMP,
price        NUMERIC(10,2),
customer_id  INT,
country      TEXT

Load the CSV into Postgres using COPY.
Tags: postgres, csv, retail.


2️⃣ load_netflix_csv_v2
Goal: Create netflix_titles table and load data from netflix_titles.csv.
Steps:
Create table with schema:
show_id       TEXT,
type          TEXT,
title         TEXT,
director      TEXT,
cast_members  TEXT,
country       TEXT,
date_added    TEXT,
release_year  INT,
rating        TEXT,
duration      TEXT,
listed_in     TEXT,
description   TEXT

Load CSV into Postgres with COPY.
Tags: postgres, csv, netflix.


🛠️ How to Run
Start Airflow (using Astronomer or Docker Compose):
astro dev start
UI: http://localhost:8080
Place CSV files inside the data/ folder.
Run the DAGs manually from the Airflow UI:
online_retail_pipeline
load_netflix_csv_v2
Verify in Postgres:
SELECT * FROM public.online_retail LIMIT 10;
SELECT * FROM public.netflix_titles LIMIT 10;


📊 Datasets
Online Retail Dataset – transaction-level data (invoices, customers, sales).
Netflix Titles Dataset – metadata about Netflix shows & movies.
Both are simplified versions taken from Kaggle datasets.













