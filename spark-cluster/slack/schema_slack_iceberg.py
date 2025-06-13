from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.avro.functions import from_avro
import requests
import argparse
import json # 스키마 정보 처리를 위해 추가
import boto3 # S3 연동을 위해 추가
from botocore.exceptions import ClientError # Boto3 예외 처리를 위해 추가

# Avro 스키마를 Schema Registry에서 가져오기
def fetch_avro_schema(schema_registry_url, subject):
    url = f"{schema_registry_url}/subjects/{subject}/versions/latest"
    response = requests.get(url)
    response.raise_for_status()
    schema_data = response.json()
    return schema_data['schema'], schema_data['version']

def get_last_known_version_from_s3(s3_client, bucket, key):
    """S3에서 마지막으로 저장된 스키마 버전을 가져옵니다."""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return int(response['Body'].read().decode('utf-8'))
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"S3에 이전 스키마 버전 파일 없음: s3://{bucket}/{key}")
            return None # 최초 실행 시 버전 정보 없음
        else:
            print(f"S3에서 스키마 버전 로드 중 오류 발생: {e}")
            raise

def save_current_version_to_s3(s3_client, bucket, key, version):
    """현재 스키마 버전을 S3에 저장합니다."""
    try:
        s3_client.put_object(Bucket=bucket, Key=key, Body=str(version).encode('utf-8'))
        print(f"스키마 버전 ({version})을 S3에 저장 완료: s3://{bucket}/{key}")
    except ClientError as e:
        print(f"S3에 스키마 버전 저장 중 오류 발생: {e}")
        raise

def send_slack_notification(webhook_url, message):
    """Slack 웹훅을 사용하여 메시지를 전송합니다. (실제 구현 필요)"""
    if webhook_url:
        response = requests.post(webhook_url, json={'text': message})
        print(f"Slack 알림 전송 시도: {response.status_code} - {response.text}")
    else:
        print("Slack Webhook URL이 없어 알림을 보내지 않습니다.")

def main():
    parser = argparse.ArgumentParser(description="Kafka to Iceberg Streaming Job")
    parser.add_argument("--schema_registry_url", required=True, help="Schema Registry URL (e.g., http://localhost:8081)")
    parser.add_argument("--schema_subject", required=True, help="Avro schema subject name in Schema Registry (e.g., userlog-avro-topic-value)")
    parser.add_argument("--kafka_brokers", required=True, help="Kafka bootstrap servers (comma-separated, e.g., host1:9092,host2:9092)")
    parser.add_argument("--kafka_topic", required=True, help="Kafka topic to subscribe to (e.g., userlog-avro-topic)")
    parser.add_argument("--iceberg_catalog_name", required=True, help="Iceberg catalog name (e.g., userlogs_catalog)")
    parser.add_argument("--iceberg_warehouse_path", required=True, help="Iceberg catalog warehouse S3 path (e.g., s3a://userlog-data/warehouse)")
    parser.add_argument("--iceberg_db_name", required=True, help="Iceberg database name (e.g., analytics)")
    parser.add_argument("--iceberg_table_name", required=True, help="Iceberg table name (e.g., user_logs)")
    parser.add_argument("--checkpoint_location", required=True, help="S3 path for Spark checkpointing (e.g., s3a://userlog-data/checkpoints/user_logs_checkpoint)")
    parser.add_argument("--s3_endpoint", required=True, help="S3 endpoint URL (e.g., http://minio:9000)")
    parser.add_argument("--s3_access_key", required=True, help="S3 access key")
    parser.add_argument("--s3_secret_key", required=True, help="S3 secret key")
    parser.add_argument("--processing_time_trigger", default="30 minutes", help="Spark streaming trigger processing time (e.g., '30 minutes', '1 hour')")
    # 스키마 버전 상태 저장 및 알림을 위한 인자 추가
    parser.add_argument("--schema_version_s3_bucket", required=True, help="스키마 버전 상태 저장을 위한 S3 버킷")
    parser.add_argument("--schema_version_s3_key", required=True, help="스키마 버전 상태 저장을 위한 S3 객체 키 (예: schema_versions/my_subject.version)")
    parser.add_argument("--slack_webhook_url", default=None, help="스키마 버전 변경 알림을 위한 Slack Webhook URL")

    args = parser.parse_args()

    # S3 클라이언트 초기화 (스키마 버전 관리용)
    # Spark의 S3 설정과 별개로, 이 스크립트에서 직접 S3에 접근하기 위함.
    s3_client_for_schema_version = boto3.client(
        's3',
        endpoint_url=args.s3_endpoint, # Spark 작업용 S3 엔드포인트와 동일하게 사용
        aws_access_key_id=args.s3_access_key,
        aws_secret_access_key=args.s3_secret_key
    )

    # Schema Registry에서 현재 스키마와 버전 가져오기
    current_avro_schema_str, current_schema_version = fetch_avro_schema(args.schema_registry_url, args.schema_subject)
    
    # S3에서 마지막으로 알려진 스키마 버전 가져오기
    last_known_schema_version = get_last_known_version_from_s3(
        s3_client_for_schema_version, args.schema_version_s3_bucket, args.schema_version_s3_key
    )

    if last_known_schema_version is None: # 최초 실행 또는 S3에 버전 정보가 없는 경우
        notification_message = (
            f"ℹ️ 최초 스키마 버전 등록 알림\n"
            f"Subject: `{args.schema_subject}` (Version: {current_schema_version}) @ `{args.schema_registry_url}/{args.schema_subject}/versions/latest`\n"
            f"현재 버전({current_schema_version})을 S3(`s3://{args.schema_version_s3_bucket}/{args.schema_version_s3_key}`)에 저장합니다."
        )
        send_slack_notification(args.slack_webhook_url, notification_message)
        save_current_version_to_s3(s3_client_for_schema_version, args.schema_version_s3_bucket, args.schema_version_s3_key, current_schema_version)
    elif current_schema_version > last_known_schema_version:
        notification_message = (
            f"🚨 새로운 스키마 버전 감지!\n"
            f"Subject: `{args.schema_subject}` @ `{args.schema_registry_url}`\n"
            f"이전 버전: {last_known_schema_version}, 새 버전: {current_schema_version}\n"
            f"새로운 버전({current_schema_version})으로 S3 상태를 업데이트합니다."
        )
        send_slack_notification(args.slack_webhook_url, notification_message)
        save_current_version_to_s3(s3_client_for_schema_version, args.schema_version_s3_bucket, args.schema_version_s3_key, current_schema_version)
    else:
        print(f"✅ 스키마 버전({current_schema_version}) 변경 없음. S3 스키마 버전 상태는 최신입니다. (마지막 확인 버전: {last_known_schema_version})")

    full_iceberg_table_name = f"{args.iceberg_catalog_name}.{args.iceberg_db_name}.{args.iceberg_table_name}"

    # Spark 세션 구성
    spark_builder = SparkSession.builder \
        .appName(f"KafkaToIceberg_{args.iceberg_db_name}_{args.iceberg_table_name}") \
        .config(f"spark.sql.catalog.{args.iceberg_catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{args.iceberg_catalog_name}.type", "hadoop") \
        .config(f"spark.sql.catalog.{args.iceberg_catalog_name}.warehouse", args.iceberg_warehouse_path) \
        .config("spark.hadoop.fs.s3a.endpoint", args.s3_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", args.s3_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", args.s3_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

    spark = spark_builder.getOrCreate()

    # Kafka 스트림 읽기
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.kafka_brokers) \
        .option("subscribe", args.kafka_topic) \
        .option("startingOffsets", "latest") \
        .load()

    # Avro 역직렬화 및 데이터 가공 (항상 Schema Registry의 최신 스키마 사용)
    parsed_df = df.select(from_avro(col("value"), current_avro_schema_str).alias("data")).select("data.*")
    final_df = parsed_df.withColumn("datestamp", to_date(col("timestamp")))

    # Iceberg 테이블에 저장
    query = final_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", args.checkpoint_location) \
        .trigger(processingTime=args.processing_time_trigger) \
        .toTable(full_iceberg_table_name)

    query.awaitTermination()

if __name__ == "__main__":
    main()
