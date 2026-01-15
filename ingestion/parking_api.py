import requests
import os
from datetime import datetime


def ingest_api_data(url, filename_prefix, output_dir="/opt/airflow/data/raw"):
    # I chose the Bulk Export endpoint because it lets me download the full dataset at once.
    # This is much faster than pagination and I don't have to worry about the 10,000 record limit.
    os.makedirs(output_dir, exist_ok=True)

    print(f"Starting bulk ingestion for: {filename_prefix}...")

    try:
        # I'm using stream=True here because for large files, I don't want to load
        # the entire thing into RAM. This keeps my memory usage low.
        with requests.get(url, stream=True, timeout=120) as response:
            response.raise_for_status()

            # I'm adding a timestamp to the filename so I can keep a history of exports
            # and avoid overwriting previous data if I run this multiple times a day.
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{filename_prefix}_{timestamp}.json"
            filepath = os.path.join(output_dir, filename)

            # I'm writing the data in 8KB chunks. This is a standard size that balances
            # memory usage and disk I/O performance efficiently.
            with open(filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            # Just a quick check to see how big the file is, so I know it actually worked.
            file_size_kb = os.path.getsize(filepath) / 1024
            print(f"SUCCESS: Saved {file_size_kb:.2f} KB to {filepath}")
            return filepath

    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed to download {url}. Reason: {e}")
        # I'm raising the error so the Airflow task knows it fialed and can retry.
        raise e
