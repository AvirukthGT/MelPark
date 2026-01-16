from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "01_debug_spark",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Test 1: Can we just run 'ls' inside the Spark container?
    # If this fails, Docker communication is broken.
    test_connection = BashOperator(
        task_id="test_simple_exec",
        bash_command="docker exec spark_master ls /opt/spark-apps",
    )

    # Test 2: Run Spark with version check (Simpler than running the whole job)
    test_spark = BashOperator(
        task_id="test_spark_version",
        bash_command="docker exec spark_master /opt/spark/bin/spark-submit --version",
    )

    test_connection >> test_spark
