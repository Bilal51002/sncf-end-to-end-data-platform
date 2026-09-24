from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def extract_data():
    print("================================")
    print("EXTRACT")
    print("Reading data from source_db")
    print("================================")


def transform_data():
    print("================================")
    print("TRANSFORM")
    print("Transforming SNCF data")
    print("================================")


def load_data():
    print("================================")
    print("LOAD")
    print("Loading data into dw_db")
    print("================================")


def quality_check():
    print("================================")
    print("QUALITY CHECK")
    print("Checking data quality")
    print("================================")


with DAG(
    dag_id="sncf_etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sncf", "etl", "data-engineering"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_data,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_data,
    )

    quality = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check,
    )

    extract >> transform >> load >> quality