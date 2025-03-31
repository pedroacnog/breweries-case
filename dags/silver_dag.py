from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

default_args = {
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
    "email_on_failure": True,
    "email": ["pedroacnog@gmail.com"],
}

with DAG(
    "silver_breweries_transformation",
    description="Data Transformation (Silver)",
    schedule="30 0 * * *", 
    start_date=pendulum.today('America/Sao_Paulo').add(days=-1),
    catchup=False,
    default_args=default_args,
    tags=["silver", "breweries"],
) as dag:

    transform_breweries = BashOperator(
        task_id="transform_breweries_silver",
        bash_command="python /opt/airflow/src/silver/transform_breweries.py",
    )

    transform_breweries
