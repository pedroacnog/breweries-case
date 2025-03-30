from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

with DAG(
    "gold_breweries_aggregations",
    description="Data Aggregation (Gold)",
    schedule="1 0 * * *", 
    start_date=pendulum.today('America/Sao_Paulo').add(days=-1),
    catchup=False,
    tags=["gold", "breweries"],
) as dag:

    aggregate_gold_views = BashOperator(
        task_id="aggregation_breweries_gold",
        bash_command="python /opt/airflow/src/gold/aggregate_breweries.py",
    )

    aggregate_gold_views
