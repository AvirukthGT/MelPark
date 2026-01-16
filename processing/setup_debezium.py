import requests
import json
import os
import time


# --- 1. First, I'm loading the Environment Variables manually ---
def load_env_file(filepath):
    if not os.path.exists(filepath):
        print(f"Warning: .env file not found at {filepath}")
        return
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, value = line.split("=", 1)
                os.environ[key.strip()] = value.strip().strip("'").strip('"')


# I'm loading the .env file from the current directory right here.
load_env_file(".env")

AZURE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_KEY = os.getenv("AZURE_STORAGE_KEY")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin_password")

if not AZURE_ACCOUNT or not AZURE_KEY:
    raise ValueError("Azure credentials missing from .env file!")

# --- 2. Next, I'm defining the Connector Configurations ---

# A. Source Connector: I'm setting this up to read from Postgres and push to Kafka.
source_config = {
    "name": "parking-sensors-source",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres_parking",
        "database.port": "5432",
        "database.user": POSTGRES_USER,
        "database.password": POSTGRES_PASSWORD,
        "database.dbname": "parking_db",
        "topic.prefix": "parking",
        "table.include.list": "public.raw_parking_sensors",
        "plugin.name": "pgoutput",
        "slot.name": "debezium_slot",
    },
}

# B. Sink Connector: Now, I'm configuring the sink to move data from Kafka to Azure Blob Storage.
# I'm keeping in mind that Debezium creates topics in the format: prefix.schema.table
topic_name = "parking.public.raw_parking_sensors"

sink_config = {
    "name": "parking-sensors-sink",
    "config": {
        "connector.class": "io.confluent.connect.azure.blob.AzureBlobStorageSinkConnector",
        "tasks.max": "1",
        "topics": topic_name,
        "flush.size": "5",  # I'm using a tiny flush size so I can see the files appear immediately!
        "azblob.account.name": AZURE_ACCOUNT,
        "azblob.account.key": AZURE_KEY,
        "azblob.container.name": "bronze",
        # I'm switching to JSON format here to make testing these files easier.
        "format.class": "io.confluent.connect.azure.blob.format.json.JsonFormat",
        "confluent.topic.bootstrap.servers": "kafka:29092",
        "confluent.topic.replication.factor": "1",
    },
}


# --- 3. Here is the function I use to register the connectors ---
def register_connector(config):
    url = "http://localhost:8083/connectors"
    headers = {"Content-Type": "application/json"}
    name = config["name"]

    try:
        # I'll check if it acts already so I don't try to create a duplicate.
        response = requests.get(f"{url}/{name}")
        if response.status_code == 200:
            print(f"Connector '{name}' already exists. Updating config...")
            resp = requests.put(
                f"{url}/{name}/config",
                headers=headers,
                data=json.dumps(config["config"]),
            )
            print(f"Update status: {resp.status_code}")
        else:
            print(f"Creating connector '{name}'...")
            resp = requests.post(url, headers=headers, data=json.dumps(config))
            print(f"Creation status: {resp.status_code} - {resp.text}")

    except Exception as e:
        print(f"Failed to connect to Debezium: {e}")


if __name__ == "__main__":
    print("--- Starting Debezium Setup ---")
    register_connector(source_config)
    time.sleep(2)  # Give it a moment
    register_connector(sink_config)
    print("--- Setup Complete ---")
    print(
        "Check status at: curl http://localhost:8083/connectors/parking-sensors-sink/status"
    )
