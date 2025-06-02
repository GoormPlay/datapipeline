def consume_kafka_to_postgres_csv(kafka_topic, pg_table, postgres_conn_id):
    import os
    import json
    import csv
    import tempfile
    from kafka import KafkaConsumer
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=os.getenv("KAFKA_BROKERS"),
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id='airflow-pg-csv-loader'
    )

    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix=".csv") as csvfile:
        writer = csv.writer(csvfile)
        for msg in consumer:
            data = msg.value
            writer.writerow([
                data.get("ad_id"),
                data.get("event_type"),
                data.get("user_id"),
                data.get("timestamp")
            ])
        csvfile.flush()

        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        hook.copy_expert(
            sql=f"COPY {pg_table} (ad_id, event_type, user_id, timestamp) FROM STDIN WITH CSV",
            filename=csvfile.name
        )
        print(f"✅ PostgreSQL COPY 완료: {csvfile.name}")

    consumer.close()

