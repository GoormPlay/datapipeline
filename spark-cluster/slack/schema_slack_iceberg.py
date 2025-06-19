from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, count, window, when, current_timestamp
import pyspark.sql.functions as func
from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.types import *
import argparse
import requests
import hashlib
import json
import boto3
import logging
try:
    from confluent_kafka.schema_registry import SchemaRegistryClient, SchemaRegistryError
except ImportError:
    SchemaRegistryClient = None
    SchemaRegistryError = None
from botocore.exceptions import ClientError
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient

def main():
    try:
        parser = argparse.ArgumentParser(description='Kafka에서 데이터를 읽어 Iceberg 테이블에 저장하는 ETL')
        parser.add_argument("--schema_registry_url", required=True, help='스키마 레지스트리 URL')
        parser.add_argument("--schema_subject", required=True, help='스키마 서브젝트 이름')
        parser.add_argument("--kafka_brokers", required=True, help='Kafka 브로커 목록 (콤마로 구분)')
        parser.add_argument("--kafka_topic", required=True, help='Kafka 토픽 이름')
        parser.add_argument("--iceberg_catalog_name", required=True, help='Iceberg 카탈로그 이름')
        parser.add_argument("--iceberg_warehouse_path", required=True, help='Iceberg 웨어하우스 경로')
        parser.add_argument("--iceberg_db_name", required=True, help='Iceberg 데이터베이스 이름')
        parser.add_argument("--iceberg_table_name", required=True, help='원본 데이터 저장할 테이블 이름')
        parser.add_argument("--checkpoint_location", required=True, help='체크포인트 위치')
        parser.add_argument("--s3_endpoint", required=True, help='S3 엔드포인트')
        parser.add_argument("--s3_access_key", required=True, help='S3 액세스 키')
        parser.add_argument("--s3_secret_key", required=True, help='S3 시크릿 키')
        parser.add_argument("--processing_time_trigger", default="30 seconds", help='처리 시간 트리거 (예: "30 seconds")')
        parser.add_argument("--schema_version_s3_bucket", required=True, help='스키마 버전 저장할 S3 버킷')
        parser.add_argument("--schema_version_s3_key", required=True, help='스키마 버전 저장할 S3 키')
        parser.add_argument("--slack_webhook_url", default=None, help='Slack 웹훅 URL')

        args = parser.parse_args()

        # S3 클라이언트 생성
        s3_client = boto3.client(
            's3',
            endpoint_url=args.s3_endpoint,
            aws_access_key_id=args.s3_access_key,
            aws_secret_access_key=args.s3_secret_key
        )
    
        # SparkSession 설정
        spark = SparkSession.builder \
            .appName("KafkaToIcebergETL") \
            .config(f"spark.sql.catalog.{args.iceberg_catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
            .config(f"spark.sql.catalog.{args.iceberg_catalog_name}.type", "hadoop") \
            .config(f"spark.sql.catalog.{args.iceberg_catalog_name}.warehouse", args.iceberg_warehouse_path) \
            .config("spark.hadoop.fs.s3a.endpoint", args.s3_endpoint) \
            .config("spark.hadoop.fs.s3a.access.key", args.s3_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", args.s3_secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .getOrCreate()

        # Iceberg 테이블 이름 정의
        raw_data_table = f"{args.iceberg_catalog_name}.{args.iceberg_db_name}.{args.iceberg_table_name}"
        video_clicks_summary_table = f"{args.iceberg_catalog_name}.{args.iceberg_db_name}.video_clicks_summary"

        # Kafka에서 데이터 읽기
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", args.kafka_brokers) \
            .option("subscribe", args.kafka_topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        # #  get magic byte value
        df = df.withColumn("magicByte", func.expr("substring(value, 1, 1)"))

        #  get schema id from value
        df = df.withColumn("valueSchemaId", func.expr("substring(value, 2, 4)"))

        # remove first 5 bytes from value
        df = df.withColumn("fixedValue", func.expr("substring(value, 6, length(value)-5)"))

        # creating a new df with magicBytes, valueSchemaId & fixedValue
        value_df = df.select("magicByte", "valueSchemaId", "fixedValue")

        # write to sink(console)
        ## trigger is used for batch interval
        # value_df \
        # .writeStream \
        # .format("console") \
        # .outputMode("append") \
        # .option("truncate", "true") \
        # .start() \
        # .awaitTermination()

    except Exception as e:
        error_message = f"애플리케이션 실행 중 예기치 않은 오류: {str(e)}"
        print(error_message)
        raise

    def get_schema_from_schema_registry(schema_registry_url, subject):
        sr = SchemaRegistryClient({'url': schema_registry_url})
        latest_version = sr.get_latest_version(subject)

        return sr, latest_version
    
    # get schema using subject name
    _, latest_version = get_schema_from_schema_registry(args.schema_registry_url, args.schema_subject)

    # deserialize data 
    fromAvroOptions = {"mode":"PERMISSIVE"}
    decoded_output = value_df.select(
        from_avro(
            func.col("fixedValue"), latest_version.schema.schema_str, fromAvroOptions
        )
        .alias("data")
    )
    value_df = decoded_output.select("data.*")
    value_df.printSchema()

    if "timestamp" in value_df.columns:
            df_eventtime = value_df.withColumn(
                "event_timestamp", 
                (col("timestamp") / 1000).cast(TimestampType())
            )
    else: # pragma: no cover
        df_eventtime = value_df.withColumn("event_timestamp", func.lit(None).cast(TimestampType()))

    df_etl = df_eventtime \
        .filter(func.col("eventType") == "content_click") \
        .withWatermark("event_timestamp", "30 minutes") \
        .groupBy(
            func.window(
                func.col("event_timestamp"),
                args.processing_time_trigger # This should be your window duration, e.g., "30 minutes"
            ),
            func.col("videoId"), # Add videoId to groupBy
            func.col("title")    # Add title to groupBy
        ) \
        .agg(func.count("*").alias("click_count")) # Example aggregation: count clicks
    
    df_etl \
        .writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", "true") \
        .start() \
        .awaitTermination()
  
if __name__ == "__main__":
    main()