from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

with DAG(
    "silver_breweries_transformation",
    description="Data Transformation (Silver)",
    schedule="30 0 * * *", 
    start_date=pendulum.today('America/Sao_Paulo').add(days=-1),
    catchup=False,
    tags=["silver", "breweries"],
) as dag:

    transform_breweries = BashOperator(
        task_id="transform_breweries_silver",
        bash_command="python /opt/airflow/src/silver/transform_breweries.py",
    )

    transform_breweries
