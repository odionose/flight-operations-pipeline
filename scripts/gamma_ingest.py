import requests 
import json
from datetime import datetime, timezone
from pathlib import Path

URL = "https://opensky-network.org/api/states/all"

def run_gamma_ingestion(**context):
    response = requests.get(URL, timeout=30)
    response.raise_for_status()  # Raise an exception for HTTP errors

    data = response.json()

    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d%H:%M:%S")

    path = Path(f"/opt/airflow/data/gamma/flights_{timestamp}.json")

    with path.open("w") as f:
        json.dump(data, f)

    context['ti'].xcom_push(key='gamma_file', value=str(path))