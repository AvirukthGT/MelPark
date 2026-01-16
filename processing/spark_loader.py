from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, to_json
from pyspark.sql.types import StructType, ArrayType
import sys
import os
import shutil


def load_env_file(filepath):
    """
    Manually load .env file into os.environ to avoid 'python-dotenv' dependency.
    """
    if not os.path.exists(filepath):
        print(f"Warning: .env file not found at {filepath}")
        return

    print(f"Loading .env from {filepath}...")
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            # Simple parsing: KEY=VALUE or KEY="VALUE"
            if "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip("'").strip('"')
                os.environ[key] = value


def run_spark_job(filename_prefix):
    # 1. Setup Paths
    # Using the path mapped in your docker-compose for the spark container
    # I'm defining my paths here. I made sure these match the volume mounts in my Docker Compose file.
    base_dir = "/opt/airflow/data"
    raw_dir = os.path.join(base_dir, "raw")
    processed_dir = os.path.join(base_dir, "processed")

    # Load .env from the same directory as this script (processing/.env)
    # I need to manually load the .env file because Docker only loads the root .env automatically.
    # This ensures my Azure credentials in processing/.env are available.
    current_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(current_dir, ".env")
    load_env_file(env_path)

    # I'm ensuring the processed directory exists so I can move files there after I'm done.
    os.makedirs(processed_dir, exist_ok=True)

    # 2. Find matching files
    # I'm scanning the raw directory to find all files related to the specific dataset I'm processing.
    all_files = [
        f
        for f in os.listdir(raw_dir)
        if f.startswith(filename_prefix) and f.endswith(".json")
    ]

    if not all_files:
        print(f"No new files found for {filename_prefix}")
        return

    # I sort the files to make sure I process them in the order they arrived.
    all_files = sorted(all_files)
    print(f"Found {len(all_files)} files to process.")

    # Fetch Azure Storage credentials from environment variables for security
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
    storage_key = os.getenv("AZURE_STORAGE_KEY")

    if not storage_account or not storage_key:
        # It's better to fail fast if credentials are missing than to try and fail later.
        raise ValueError(
            "Azure Storage credentials are not set in environment variables!"
        )

    # 3. Initialize Spark with Postgres & Azure Config
    # I'm configuring Spark here. I need to make sure the Postgres JDBC driver is available
    # both on the driver and the executors so I can write to the database.
    # Azure Storage credentials are passed securely via Spark config, using environment variables.
    # 3. Initialize Spark with Postgres & Azure Config
    spark = (
        SparkSession.builder.appName(f"Loader_{filename_prefix}")
        .master("local[*]")
        .config(
            "spark.jars",
            "/opt/spark-jars/postgresql-42.7.2.jar,/opt/spark-jars/hadoop-azure-3.3.4.jar,/opt/spark-jars/azure-storage-8.6.6.jar,/opt/spark-jars/jetty-util-ajax-9.4.52.v20230823.jar,/opt/spark-jars/jetty-util-9.4.52.v20230823.jar",
        )
        .config(
            "spark.driver.extraClassPath",
            "/opt/spark-jars/postgresql-42.7.2.jar:/opt/spark-jars/hadoop-azure-3.3.4.jar:/opt/spark-jars/azure-storage-8.6.6.jar:/opt/spark-jars/jetty-util-ajax-9.4.52.v20230823.jar:/opt/spark-jars/jetty-util-9.4.52.v20230823.jar",
        )
        .getOrCreate()
    )

    # FIX 1: Configure for DFS (Data Lake) endpoint
    spark.conf.set(
        f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key
    )

    # Determine loading strategy
    if "sensor" in filename_prefix:
        # Since sensor data is a stream of events, I want to keep everything history-wise.
        # So I'm queueing up ALL the files I found to be appended.
        files_to_load = all_files
        mode = "append"
        print("Sensor Data: Incremental Append mode.")
    else:
        # For static tables, we only want the absolute latest snapshot
        # For static reference data (like parking bays), I only care about the latest state.
        # So I'm picking just the last file and I'll overwrite the table.
        files_to_load = [all_files[-1]]
        mode = "overwrite"
        print("Static Data: Full Snapshot Overwrite mode.")

    full_paths = [f"file://{os.path.join(raw_dir, f)}" for f in files_to_load]

    try:
        # Read JSON (multiLine=true for pretty-printed files)
        # I'm using multiLine=true because my input JSON files are pretty-printed.
        df = spark.read.option("multiLine", "true").json(full_paths)

        # Add metadata and deduplicate
        # I'm adding a timestamp to track ingestion time and removing any exact duplicates
        # that might have slipped in.
        df = df.withColumn("_ingested_at", current_timestamp()).dropDuplicates()

        # --- FIX: Convert Complex Types to JSON Strings ---
        # This is critical for Postgres to accept nested data
        # I iterate through the schema and if I see any arrays or structs, I convert them
        # to JSON strings. This is the only way Postgres will accept them without complaining.
        for field in df.schema.fields:
            if isinstance(field.dataType, (StructType, ArrayType)):
                print(f"Converting complex column '{field.name}' to JSON string...")
                df = df.withColumn(field.name, to_json(col(field.name)))

        # 5. Write to Postgres (The 'Source' DB container)
        # Note: We use the container name 'postgres_parking'
        jdbc_url = "jdbc:postgresql://postgres_parking:5432/parking_db"
        table_name = f"raw_{filename_prefix}"

        # I'm verifying that I can fetch the credentials from the environment variables.
        # This is much safer than hardcoding passwords in the script.
        user = os.getenv("POSTGRES_USER", "admin")
        password = os.getenv("POSTGRES_PASSWORD", "admin_password")

        db_properties = {
            "user": user,
            "password": password,
            "driver": "org.postgresql.Driver",
            "stringtype": "unspecified",  # Helps Postgres infer JSON types from strings
        }

        print(f"Writing to {table_name}...")
        # .coalesce(1) ensures we don't open 20 parallel connections to Postgres
        # I'm using coalesce(1) here to reduce the number of connections to my database,
        # since I don't want to overwhelm it with too many concurrent writes.
        df.coalesce(1).write.jdbc(
            url=jdbc_url, table=table_name, mode=mode, properties=db_properties
        )
        print("Write Successful.")

        # 6. Sink B: Write to Azure Blob Storage (Archive Storage)
        # Partitioning by ingest_date to handle snapshots over time
        azure_container = "bronze"
        # Using the storage_account fetched from environment variables for security
        azure_path = (
            f"abfss://bronze@{storage_account}.dfs.core.windows.net/{filename_prefix}"
        )

        df.write.mode(mode).parquet(azure_path)
        print("Dual-Write Successful.")

    except Exception as e:
        print(f"Job Failed: {e}")
        raise e
    finally:
        spark.stop()

    # 7. Housekeeping: Move processed files
    # I'm moving the files to the 'processed' directory so they don't get picked up again
    # in the next run. This keeps my ingestion idempotent.
    print("Moving files to 'processed' folder...")
    for f in all_files:
        src = os.path.join(raw_dir, f)
        dst = os.path.join(processed_dir, f)
        if os.path.exists(dst):
            os.remove(dst)
        shutil.move(src, dst)

    print(f"Moved {len(all_files)} files. Job Complete.")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_spark_job(sys.argv[1])
