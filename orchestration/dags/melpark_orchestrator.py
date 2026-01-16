from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago

# --- CONFIGURATION ---
# Replace these with the actual Job IDs you copied from Databricks
SILVER_JOB_ID = 894005933928942
GOLD_JOB_ID = 275702669994941

default_args = {
    "owner": "krokadil",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
}

with DAG(
    "melbourne_parking_pipeline",
    default_args=default_args,
    description="Orchestrate Silver (Spark) and Gold (dbt) on Azure Databricks",
    schedule_interval="@daily",  # Runs once a day
    start_date=days_ago(1),
    catchup=False,
    tags=["azure", "databricks", "dbt"],
) as dag:

    # 1. Trigger the Silver Layer Notebook
    run_silver = DatabricksRunNowOperator(
        task_id="trigger_silver_layer",
        databricks_conn_id="databricks_default",  # We will set this in UI next
        job_id=SILVER_JOB_ID,
    )

    # 2. Trigger the Gold Layer dbt Job
    run_gold = DatabricksRunNowOperator(
        task_id="trigger_gold_dbt",
        databricks_conn_id="databricks_default",
        job_id=GOLD_JOB_ID,
    )

    # 3. Define the dependency (Silver -> Gold)
    run_silver >> run_gold
