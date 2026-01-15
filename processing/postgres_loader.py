import os
import pandas as pd
from sqlalchemy import create_engine, text
from airflow.hooks.base import BaseHook

def load_to_postgres(filename_prefix, **kwargs):
    input_dir = "/opt/airflow/data/raw"
    
    # 1. Find the latest file
    # I'm looking for the most recent file because I only care about loading the latest data.
    files = [f for f in os.listdir(input_dir) if f.startswith(filename_prefix) and f.endswith(".json")]
    if not files:
        print(f"No files found for {filename_prefix}")
        return

    latest_file = sorted(files)[-1]
    filepath = os.path.join(input_dir, latest_file)
    print(f"Processing: {filepath}")

    # 2. Connect to DB
    # I'm using the Airflow hook here so I don't have to hardcode my database credentials.
    conn = BaseHook.get_connection("postgres_parking_conn")
    db_url = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    engine = create_engine(db_url)

    # 3. Read Data
    try:
        df = pd.read_json(filepath)
        
        # Clean up lists/dicts
        # I found that pandas has trouble writing nested lists/dicts to SQL, so I'm converting 
        # them to strings here to avoid errors.
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                df[col] = df[col].astype(str)
        
        # I'm adding an ingestion timestamp so I know when this record was loaded into my DB.
        df['_ingested_at'] = pd.Timestamp.now()
    except ValueError as e:
        print(f"Error reading JSON: {e}")
        raise e

    # 4. Smart Loading Strategy
    table_name = f"raw_{filename_prefix}"
    
    with engine.connect() as connection:
        # STRATEGY A: Full Overwrite (For Meters, Bays, Zones)
        # If the data is a "snapshot" of the current state, just wipe and reload.
        # I'm doing a full refresh here because these tables are relatively small and represent
        # the current state of the world, not a history of events.
        if "sensor" not in filename_prefix:
            print(f"Performing FULL REFRESH on {table_name}...")
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print("Success.")

        # STRATEGY B: Incremental Upsert (For Sensors)
        # If the data is a "history" (events), we must not duplicate events.
        # For sensor data, I need to be careful not to create duplicates, so I'm using a
        # staging table approach.
        else:
            print(f"Performing INCREMENTAL LOAD on {table_name}...")
            
            # Step 1: Create a temporary staging table
            # I'm writing to a staging table first so I can compare it against the main table.
            staging_table = f"staging_{filename_prefix}"
            df.to_sql(staging_table, engine, if_exists='replace', index=False)
            
            # Step 2: Delete duplicates from the MAIN table if they exist in STAGING
            # (Assuming 'bay_id' and 'last_updated' form a unique key - verify your data!)
            # If you don't have a clear unique key, use ALL columns.
            
            # For simplicity, we will assume rows are unique if ALL columns match.
            # I'm deleting any records from the main table that also exist in the staging table
            # (based on these keys) to prevent duplicates before inserting the new data.
            delete_query = text(f"""
                DELETE FROM {table_name}
                USING {staging_table}
                WHERE {table_name}.bay_id = {staging_table}.bay_id 
                AND {table_name}.st_marker_id = {staging_table}.st_marker_id
                AND {table_name}.last_updated = {staging_table}.last_updated;
            """)
            
            # Check if table exists before trying to delete
            # I need to check if the table exists first, otherwise my DELETE query will crash
            # if this is the first run.
            table_exists = connection.execute(text(f"SELECT to_regclass('{table_name}')")).scalar()
            
            if table_exists:
                print("Removing potential duplicates...")
                connection.execute(delete_query)
            
            # Step 3: Insert new rows from Staging to Main
            print("Inserting new rows...")
            df.to_sql(table_name, engine, if_exists='append', index=False)
            
            # Step 4: Drop Staging
            # I'm cleaning up after myself by dropping the staging table.
            connection.execute(text(f"DROP TABLE {staging_table}"))
            print("Success.")
