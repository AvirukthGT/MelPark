import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator  # <--- NEW IMPORT
from datetime import datetime
from ingestion.parking_api import ingest_api_data

# Note: We REMOVED 'from processing.postgres_loader import load_to_postgres' 
# because we are using Spark now.

API_SOURCES = [
    {
        "name": "parking_sensors",
        "url": "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/on-street-parking-bay-sensors/exports/json",
    },
    {
        "name": "parking_bays",
        "url": "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/on-street-parking-bays/exports/json",
    },
    {
        "name": "parking_zones_plates",
        "url": "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/sign-plates-located-in-each-parking-zone/exports/json",
    },
    {
        "name": "parking_restrictions",
        "url": "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/on-street-car-park-bay-restrictions/exports/json",
    },
    {
        "name": "parking_meters",
        "url": "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/on-street-car-parking-meters-with-location/exports/json",
    },
]

with DAG(
    dag_id="melbourne_parking_elt_spark", # Changed ID to indicate Spark
    start_date=datetime(2026, 1, 14),
    schedule_interval="@daily",
    catchup=False,
    tags=["melbourne", "elt", "spark"],
    
) as dag:

    for source in API_SOURCES:

        # Task 1: Ingest (Stays the same - Python downloads JSON to /data/raw)
        task_ingest = PythonOperator(
            task_id=f"ingest_{source['name']}",
            python_callable=ingest_api_data,
            op_kwargs={"url": source["url"], "filename_prefix": source["name"]},
        )

        # Task 2: Load (UPDATED - Triggers Spark Container)
        # This command tells the 'spark_master' container to run your script
        # Task 2: Load (One-line version to prevent formatting errors)
        spark_submit_cmd = (
            f"PYTHONPATH=/opt/airflow:/home/airflow/.local/lib/python3.8/site-packages "
            f"python3 /opt/airflow/processing/spark_loader.py {source['name']}"
        )

        task_load = BashOperator(
            task_id=f"load_spark_{source['name']}",
            bash_command=spark_submit_cmd
        )

        # Dependency
        task_ingest >> task_load