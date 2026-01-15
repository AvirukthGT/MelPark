import sys
import os

# 1. Add the project root to Python's search path
# I need to do this so Airflow can find my 'ingestion' module, which is in the parent directory.
sys.path.append("/opt/airflow")
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from ingestion.parking_api import ingest_api_data 
from processing.postgres_loader import load_to_postgres
# Note: I removed the '?limit=20' from these URLs because the script adds it manually
# I'm defining my API sources here. This EXPORTS endpoint is great because it gives me everything 
# without needing to paginate.
API_SOURCES = [
    {
        "name": "parking_sensors", 
        "url": "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/on-street-parking-bay-sensors/exports/json"
    },
    {
        "name": "parking_bays", 
        "url": "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/on-street-parking-bays/exports/json"
    },
    {
        "name": "parking_zones_plates", 
        "url": "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/sign-plates-located-in-each-parking-zone/exports/json"
    },
    {
        "name": "parking_restrictions", 
        "url": "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/on-street-car-park-bay-restrictions/exports/json"
    },
    {
        "name": "parking_meters", 
        "url": "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/on-street-car-parking-meters-with-location/exports/json"
    }
]

# I'm setting up the DAG here. I set catchup=False because I don't want to run a bunch of old 
# backfills if I pause the DAG.
with DAG(
    dag_id='melbourne_parking_elt',
    start_date=datetime(2026, 1, 14),
    schedule_interval='@daily',
    catchup=False,
    tags=['melbourne', 'elt']
) as dag:

    for source in API_SOURCES:
        
        # Task 1: Get Data
        task_ingest = PythonOperator(
            task_id=f"ingest_{source['name']}",
            python_callable=ingest_api_data,
            op_kwargs={
                "url": source['url'],
                "filename_prefix": source['name']
            }
        )

        # Task 2: Load Data
        task_load = PythonOperator(
            task_id=f"load_{source['name']}",
            python_callable=load_to_postgres,
            op_kwargs={
                "filename_prefix": source['name']
            }
        )

        # Dependency
        task_ingest >> task_load