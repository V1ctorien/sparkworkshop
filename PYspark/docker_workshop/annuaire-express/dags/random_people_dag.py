from datetime import datetime, timedelta

from airflow.sdk import DAG                                          # Airflow 3
from airflow.providers.standard.operators.python import PythonOperator

from stream_to_kafka import start_streaming

with DAG(
    dag_id="random_people_names",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",        # toutes les 5 min (schedule_interval n'existe plus)
    catchup=False,                   # ne rattrape pas les runs depuis janvier
    max_active_runs=1,               # jamais deux producteurs en même temps
    default_args={"owner": "airflow", "retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["tuto", "kafka"],
) as dag:
    PythonOperator(
        task_id="kafka_data_stream",
        python_callable=start_streaming,
        op_kwargs={"duration_s": 120, "every_s": 10},
    )