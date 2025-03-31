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
    "gold_breweries_aggregations",
    description="Data Aggregation (Gold)",
    schedule="1 0 * * *", 
    start_date=pendulum.today('America/Sao_Paulo').add(days=-1),
    catchup=False,
    default_args=default_args,
    tags=["gold", "breweries"],
) as dag:

    aggregate_gold_views = BashOperator(
        task_id="aggregation_breweries_gold",
        bash_command="python /opt/airflow/src/gold/aggregate_breweries.py",
    )

    aggregate_gold_views
