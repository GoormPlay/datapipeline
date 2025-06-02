from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from jobs.kafka.consume_ad_pg import consume_kafka_to_postgres_csv
from utils.slack_fail_noti import task_fail_slack_alert


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=15),
}

with DAG(
    dag_id='ad_logs_to_postgres_csv',
    default_args=default_args,
    description='Consume Kafka ad-events and bulk insert to PostgreSQL using COPY',
    schedule_interval='*/10 * * * *',  # 매 10분
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=['ad', 'postgres', 'copy']
) as dag_postgres:

    to_postgres_csv = PythonOperator(
        task_id='consume_kafka_and_store_postgres_csv',
        python_callable=consume_kafka_to_postgres_csv,
        op_kwargs={
            'kafka_topic': 'ad-events',
            'pg_table': 'ad_billing_db',
            'postgres_conn_id': 'postgres'
        },
        on_failure_callback=task_fail_slack_alert
    )

    to_postgres_csv