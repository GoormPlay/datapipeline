from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from jobs.kafka.consume_ad_minio import consume_user_events
from utils.slack_fail_noti import task_fail_slack_alert

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=15),
}

# DAG 1: Kafka → MinIO (raw log 저장)
with DAG(
    dag_id='ad_logs_to_minio',
    default_args=default_args,
    description='Consume Kafka ad-events and store raw logs to MinIO',
    schedule_interval='*/30 * * * *',  # 매 30분
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=['ad', 'minio', 'batch']
) as dag_minio:

    to_minio = PythonOperator(
        task_id='consume_kafka_and_store_minio',
        python_callable=consume_user_events,
        op_kwargs={
            'execution_date_str': '{{ ds }}',
            'minio_conn_id': 'minio',
            'bucket_name': 'ml-user-log'
        },
        on_failure_callback=task_fail_slack_alert
    )

    to_minio