import json
import pandas as pd
from pathlib import Path

def run_beta_transform(**context):
    execution_date = context["ds_nodash"]

    gamma_file=  context['ti'].xcom_pull(
        key='gamma_file',
        task_ids='gamma_ingest'
    )

    if not gamma_file:
        raise ValueError("Gamma file path not found in XCom")
       

    beta_file_path = Path("/opt/airflow/data/beta")
    beta_file_path.mkdir(parents=True, exist_ok=True)

    with open(gamma_file, "r") as f:
        data = json.load(f)

    df_raw = pd.DataFrame(data["states"])

    df_raw.columns = [
        "icao24",
        "callsign",
        "origin_country",
        "time_position",
        "last_contact",
        "longitude",
        "latitude",
        "baro_altitude",
        "on_ground",
        "velocity",
        "true_track",
        "vertical_rate",
        "sensors",
        "geo_altitude",
        "squawk",
        "spi",
        "position_source"
    ]

    df = df_raw [
        [
            "icao24",
            "origin_country",
            "velocity",
            "geo_altitude",
        ]
    ]

    output_file = beta_file_path / f"beta_flights_{execution_date}.csv"
    df.to_csv(output_file, index=False)

    context['ti'].xcom_push(key='beta_file', value=str(output_file))

