from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from utils.slack_fail_noti import task_fail_slack_alert
from jobs.kafka.consume_user_events import consume_user_events
from jobs.kafka.check_brokers import check_kafka_broker_health

with DAG(
    dag_id='kafka_to_minio_to_spark',
    start_date=datetime(2025, 6,1),
    schedule_interval='0 * * * *',
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'execution_timeout': timedelta(minutes=20),
    },
    tags=['user-activity-log', 'ml'],
) as dag:

    check_kafka_brokers = PythonOperator(
        task_id='check_kafka_brokers',
        python_callable=check_kafka_broker_health,
        on_failure_callback=task_fail_slack_alert
    )

    consume_kafka = PythonOperator(
        task_id='consume_kafka_user_events',
        python_callable=lambda **context: consume_user_events(context['ds']),
        on_failure_callback=task_fail_slack_alert
    )

    wait_for_file = S3KeySensor(
        task_id='wait_for_file',
        bucket_name='ml-user-log',
        bucket_key="{{ ds }}.json",
        aws_conn_id='minio',
        poke_interval=10,
        timeout=600,
        on_failure_callback=task_fail_slack_alert
    )

    run_spark_etl = SparkSubmitOperator(
        task_id='run_spark_etl',
        application="/opt/airflow/jobs/spark/user_events_spark.py",
        conn_id='spark',
        jars="/opt/spark/jars/hadoop-aws-3.3.1.jar,/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar,/opt/spark/jars/postgresql-42.7.4.jar",
        on_failure_callback=task_fail_slack_alert
    )

    check_kafka_brokers >> consume_kafka >> wait_for_file >> run_spark_etl
