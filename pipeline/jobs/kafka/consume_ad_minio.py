def consume_user_events(execution_date_str: str, minio_conn_id='minio', bucket_name='ad-billing-log'):
    import os
    import json
    from datetime import datetime
    from kafka import KafkaConsumer
    from kafka.structs import OffsetAndMetadata
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    execution_date = datetime.strptime(execution_date_str, "%Y-%m-%d")
    filename = execution_date.strftime("%Y-%m-%d") + ".json"

    consumer = KafkaConsumer(
        'content-user-events',
        bootstrap_servers=os.getenv("KAFKA_BROKERS"),
        group_id='aiflow-billing-minio',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=5000
    )

    messages = consumer.poll(timeout_ms=10000)

    os.makedirs("/tmp/kafka_billing_logs", exist_ok=True)
    local_path = f"/tmp/kafka_billing_logs/{filename}"

    with open(local_path, "w") as f:
        for tp, msgs in messages.items():
            for msg in msgs:
                event = msg.value
                json.dump(event, f)
                f.write("\n")
                consumer.commit(offsets={tp: OffsetAndMetadata(msg.offset + 1, None)})

    # Upload to MinIO
    s3_hook = S3Hook(aws_conn_id=minio_conn_id)
    s3_hook.load_file(local_path, f"{filename}", bucket_name, replace=True)
