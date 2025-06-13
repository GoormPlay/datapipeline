from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.avro.functions import from_avro
import requests
import argparse

# Avro 스키마를 Schema Registry에서 가져오기
def fetch_avro_schema(schema_registry_url, subject):
    url = f"{schema_registry_url}/subjects/{subject}/versions/latest"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()['schema']

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

    args = parser.parse_args()

    # Schema Registry에서 스키마 가져오기
    avro_schema_str = fetch_avro_schema(args.schema_registry_url, args.schema_subject)
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

    # Avro 역직렬화 및 데이터 가공
    parsed_df = df.select(from_avro(col("value"), avro_schema_str).alias("data")).select("data.*")
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
