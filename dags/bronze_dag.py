from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

with DAG(
    "bronze_breweries_ingestion",
    description="API Data Ingestion (Bronze)",
    schedule="0 0 * * *", 
    start_date=pendulum.today('America/Sao_Paulo').add(days=-1),
    catchup=False,
    tags=["bronze", "breweries"],
) as dag:

    ingest_breweries = BashOperator(
        task_id="get_breweries_from_api",
        bash_command="python /opt/airflow/src/bronze/extract_breweries.py",
    )

    ingest_breweries
