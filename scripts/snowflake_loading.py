import pandas as pd
import snowflake.connector
from airflow.hooks.base import BaseHook


def run_snowflake_loading(**context):
    # Retrieve Snowflake connection details from Airflow connections
    conn = BaseHook.get_connection("flights_snowflake")
    
    # Establish connection to Snowflake
    snowflake_conn = snowflake.connector.connect(
        user=conn.login,
        password=conn.password,
        account=conn.extra_dejson.get("account"),
        warehouse=conn.extra_dejson.get("warehouse"),
        database=conn.extra_dejson.get("database"),
        schema=conn.schema,
        role=conn.extra_dejson.get("role")
    )
    
    # Load the aggregated data from the previous task
    alpha_file = context["ti"].xcom_pull(key="alpha_file", task_ids="alpha_aggregate")
    if not alpha_file:
        raise ValueError("Alpha file path not found in XCom")
    
    execution_date = context["data_interval_start"].strftime("%Y-%m-%d %H:%M:%S")
    
    df = pd.read_csv(alpha_file)
    
    # Create a cursor and load data into Snowflake
    cursor = snowflake_conn.cursor()
    
    merge_query = """
            MERGE INTO FLIGHT_KPIS target
            USING (
                SELECT 
                   TO_TIMESTAMP(%s) AS WINDOW_START,
                   %s AS ORIGIN_COUNTRY,
                   %s AS TOTAL_FLIGHTS,
                   %s AS AVG_VELOCITY,
                   %s AS ON_GROUND
            ) source
            ON target.WINDOW_START = source.WINDOW_START
            AND target.ORIGIN_COUNTRY = source.ORIGIN_COUNTRY
            WHEN MATCHED THEN
                UPDATE SET
                    target.TOTAL_FLIGHTS = source.TOTAL_FLIGHTS,
                    target.AVG_VELOCITY = source.AVG_VELOCITY,
                    target.ON_GROUND = source.ON_GROUND,
                    LOAD_TIME = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (WINDOW_START, ORIGIN_COUNTRY, TOTAL_FLIGHTS, AVG_VELOCITY, ON_GROUND)
                VALUES (source.WINDOW_START, source.ORIGIN_COUNTRY, source.TOTAL_FLIGHTS, source.AVG_VELOCITY, source.ON_GROUND)
        """
    
  
    with snowflake_conn.cursor() as cursor:
            for _, row in df.iterrows():
                cursor.execute(
                    merge_query,
                    (
                        execution_date,
                        row["origin_country"],
                        int(row["total_flights"]),
                        float(row["avg_velocity"]),
                        int(row["avg_on_ground"])
                    )
                )

    snowflake_conn.close()