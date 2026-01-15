from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, col, to_json
from pyspark.sql.types import StructType, ArrayType
import sys
import os
import shutil

def run_spark_job(filename_prefix):
    # 1. Setup Paths
    base_dir = "/opt/spark-data"
    raw_dir = os.path.join(base_dir, "raw")
    processed_dir = os.path.join(base_dir, "processed")
    
    # Create processed dir if not exists
    # I'm ensuring the processed directory exists so I can move files there after I'm done.
    os.makedirs(processed_dir, exist_ok=True)

    # 2. Find ALL matching files in Raw
    # I'm grabbing everything matching the prefix (e.g., all 10 'parking_bays_xx.json' files)
    # I'm scanning the raw directory to find all files related to the specific dataset I'm processing.
    all_files = [f for f in os.listdir(raw_dir) if f.startswith(filename_prefix) and f.endswith(".json")]
    
    if not all_files:
        print(f"No new files found for {filename_prefix}")
        return

    # Sort them by time (filename usually has timestamp)
    # I sort the files to make sure I process them in the order they arrived.
    all_files = sorted(all_files)
    
    print(f"Found {len(all_files)} files to process.")

    # 3. Initialize Spark
    # I'm starting the Spark session here. I strictly need the PostgreSQL driver jar 
    # to be able to write to the database later.
    spark = SparkSession.builder \
        .appName(f"Loader_{filename_prefix}") \
        .config("spark.jars", "/opt/spark-jars/postgresql-42.7.2.jar") \
        .getOrCreate()

    # --- STRATEGY: STATIC vs. SENSOR ---
    
    files_to_load = []
    mode = ""

    if "sensor" in filename_prefix:
        # SENSORS (Incremental): We need history.
        # Load ALL files found in raw.
        # Since sensor data is a stream of events, I want to keep everything history-wise.
        # So I'm queueing up ALL the files I found to be appended.
        files_to_load = all_files
        mode = "append"
        print("Sensor Data detected: Loading ALL new files (Incremental).")
    else:
        # STATIC (Snapshot): We hate duplicates.
        # Only load the ONE latest file. Ignore the older 9 files (they are obsolete).
        # For static reference data (like parking bays), I only care about the latest state.
        # So I'm picking just the last file and I'll overwrite the table.
        files_to_load = [all_files[-1]] 
        mode = "overwrite"
        print("Static Data detected: Loading ONLY the latest file (Snapshot).")

    # 4. Read Data
    # Spark can read a list of specific paths
    full_paths = [f"file://{os.path.join(raw_dir, f)}" for f in files_to_load]
    
    try:
        # read.json accepts a list of paths
        # I'm ignoring the single-line default and using multiLine=true because 
        # my JSON files are pretty-printed.
        df = spark.read.option("multiLine", "true").json(full_paths)
        
        # Add metadata
        # I'm adding an ingestion timestamp so I can track when this batch was processed.
        df = df.withColumn("_ingested_at", current_timestamp())
        
        # Simple dedupe (remove exact row duplicates inside the batch)
        # I'm doing a quick deduplication just in case the source sent the same record twice.
        df = df.dropDuplicates()

        # ---------------------------------------------------------
        # NEW FIX: Convert Complex Types (Struct/Array) to JSON Strings
        # ---------------------------------------------------------
        # This prevents the "Can't get JDBC type for struct" error.
        # Postgres can't handle nested objects natively, so we stringify them.
        # I iterate through the schema and if I see any arrays or structs, I convert them
        # to JSON strings. This is the only way Postgres will accept them.
        for field in df.schema.fields:
            if isinstance(field.dataType, (StructType, ArrayType)):
                print(f"Converting complex column '{field.name}' to JSON string...")
                df = df.withColumn(field.name, to_json(col(field.name)))
        # ---------------------------------------------------------

        # 5. Write to Postgres
        jdbc_url = "jdbc:postgresql://postgres_parking:5432/parking_db"
        table_name = f"raw_{filename_prefix}"
        
        db_properties = {
            "user": "admin",
            "password": "admin_password",
            "driver": "org.postgresql.Driver"
        }

        print(f"Writing to {table_name} with mode='{mode}'...")
        # I'm finally writing the dataframe to Postgres using the JDBC driver.
        df.write.jdbc(url=jdbc_url, table=table_name, mode=mode, properties=db_properties)
        print("Write Successful.")

    except Exception as e:
        print(f"Job Failed: {e}")
        spark.stop()
        raise e

    spark.stop()

    # 6. Housekeeping: Move Files
    # CRITICAL: I move ALL files we found (even the ones we skipped for static data)
    # so that the raw folder is empty for the next run.
    # I'm moving the files to the 'processed' directory so they don't get picked up again
    # in the next run. This keeps my ingestion idempotent.
    print("Moving files to 'processed' folder...")
    for f in all_files:
        src = os.path.join(raw_dir, f)
        dst = os.path.join(processed_dir, f)
        
        # Handle overwrite if file exists in processed
        if os.path.exists(dst):
            os.remove(dst)
            
        shutil.move(src, dst)
        
    print(f"Moved {len(all_files)} files. Job Complete.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_spark_job(sys.argv[1])
