import sys
import os

# 1. Add the project root to Python's search path
sys.path.append("/opt/airflow")
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from ingestion.parking_api import ingest_api_data 

# Note: We removed the '?limit=20' from these URLs because the script adds it manually
# Updated Source List using the EXPORTS endpoint
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

with DAG(
    dag_id='melbourne_parking_ingestion',
    start_date=datetime(2026, 1, 14),
    schedule_interval='@daily',
    catchup=False,
    tags=['ingestion', 'melbourne']
) as dag:

    for source in API_SOURCES:
        PythonOperator(
            task_id=f"ingest_{source['name']}",
            python_callable=ingest_api_data,
            op_kwargs={
                "url": source['url'],
                "filename_prefix": source['name']
            }
        )