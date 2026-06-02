import pandas as pd
from pathlib import Path

def run_alpha_aggregate(**context):
    beta_file = context["ti"].xcom_pull(key="beta_file", task_ids="beta_transform")
    if not beta_file:
        raise ValueError("Beta file path not found in XCom")
    
    df = pd.read_csv(beta_file)

    aggregate_df = df.groupby("origin_country").agg(
        total_flights=pd.NamedAgg(column="icao24", aggfunc="count"),
        avg_velocity=pd.NamedAgg(column="velocity", aggfunc="mean"),
        avg_on_ground=pd.NamedAgg(column="on_ground", aggfunc="mean")
    ).reset_index()

    alpha_file_path = Path(beta_file.replace("beta", "alpha").replace("beta_flights", "alpha_aggregate_flights"))
    context['ti'].xcom_push(key='alpha_file', value=str(alpha_file_path))
    aggregate_df.to_csv(alpha_file_path, index=False)