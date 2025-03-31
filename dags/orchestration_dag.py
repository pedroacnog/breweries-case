from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

with DAG(
    "orchestration_breweries_pipeline",
    description="Full Medallion pipeline: Bronze > Silver > Gold",
    schedule="0 2 * * *", 
    start_date=pendulum.today("America/Sao_Paulo").add(days=-1),
    catchup=False,
    tags=["orchestration", "breweries", "full"],
) as dag:

    bronze_task = BashOperator(
        task_id="extract_breweries",
        bash_command="python /opt/airflow/src/bronze/extract_breweries.py",
    )

    silver_task = BashOperator(
        task_id="transform_breweries",
        bash_command="python /opt/airflow/src/silver/transform_breweries.py",
    )

    gold_task = BashOperator(
        task_id="aggregate_breweries",
        bash_command="python /opt/airflow/src/gold/aggregate_breweries.py",
    )

    bronze_task >> silver_task >> gold_task
