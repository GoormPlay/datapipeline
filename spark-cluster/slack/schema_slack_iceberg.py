from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit, current_timestamp, count # lit 추가
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import *
import argparse
import requests
import hashlib
import json
import boto3
from botocore.exceptions import ClientError

### 스키마 유틸 ###

def avro_type_to_spark_type(avro_type):
    if isinstance(avro_type, str):
        return {
            "string": StringType(),
            "int": IntegerType(),
            "long": LongType(),
            "boolean": BooleanType(),
            "float": FloatType(),
            "double": DoubleType(),
            "bytes": BinaryType()
        }.get(avro_type, StringType())
    if isinstance(avro_type, list):
        non_null = [t for t in avro_type if t != "null"]
        return avro_type_to_spark_type(non_null[0]) if non_null else StringType()
    if isinstance(avro_type, dict):
        type_ = avro_type["type"]
        if type_ == "record":
            return StructType([
                StructField(f["name"], avro_type_to_spark_type(f["type"]))
                for f in avro_type["fields"]
            ])
        elif type_ == "array":
            return ArrayType(avro_type_to_spark_type(avro_type["items"]))
        elif type_ == "map":
            return MapType(StringType(), avro_type_to_spark_type(avro_type["values"]))
    return StringType()

def avro_json_to_spark_schema(avro_schema_str: str) -> StructType:
    avro_json = json.loads(avro_schema_str)
    return StructType([
        StructField(f["name"], avro_type_to_spark_type(f["type"]), True)
        for f in avro_json["fields"]
    ])

### 스키마 관리 ###

def fetch_avro_schema(schema_registry_url, subject):
    url = f"{schema_registry_url}/subjects/{subject}/versions/latest"
    response = requests.get(url)
    response.raise_for_status()
    schema_data = response.json()
    return schema_data['schema'], schema_data['version']

def get_schema_hash(schema_str):
    schema_dict = json.loads(schema_str)
    normalized = json.dumps(schema_dict, sort_keys=True)
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()

def get_last_schema_hash_from_s3(s3_client, bucket, key):
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        return obj['Body'].read().decode('utf-8')
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return None
        else:
            raise

def save_schema_hash_to_s3(s3_client, bucket, key, schema_hash):
    s3_client.put_object(Bucket=bucket, Key=key, Body=schema_hash.encode('utf-8'))

def save_schema_to_s3(s3_client, bucket, key, schema_str):
    s3_client.put_object(Bucket=bucket, Key=key, Body=schema_str.encode('utf-8'))

def send_slack_notification(webhook_url, message):
    if webhook_url:
        try:
            response = requests.post(webhook_url, json={'text': message}, timeout=10) # 타임아웃 추가
            if response.status_code == 200:
                print(f"✅ Slack 알림 전송 성공 (상태 코드: {response.status_code})")
            else:
                # 개인 정보 보호를 위해 URL의 일부만 로깅
                safe_url = webhook_url[:webhook_url.find('hooks.slack.com/')+len('hooks.slack.com/')] + '...' if 'hooks.slack.com/' in webhook_url else "URL 형식 오류"
                print(f"❌ Slack 알림 전송 실패 (상태 코드: {response.status_code})")
                print(f"    Webhook URL (일부): {safe_url}")
                print(f"    응답 내용: {response.text}") # Slack의 응답 내용 출력
        except requests.exceptions.RequestException as e:
            print(f"❌ Slack 알림 전송 중 예외 발생: {e}")
    else:
        print("⚠️ Slack Webhook URL이 없어 알림 생략됨")

### MAIN ###

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema_registry_url", required=True)
    parser.add_argument("--schema_subject", required=True)
    parser.add_argument("--kafka_brokers", required=True)
    parser.add_argument("--kafka_topic", required=True)
    parser.add_argument("--iceberg_catalog_name", required=True)
    parser.add_argument("--iceberg_warehouse_path", required=True)
    parser.add_argument("--iceberg_db_name", required=True)
    parser.add_argument("--iceberg_table_name", required=True)
    parser.add_argument("--checkpoint_location", required=True)
    parser.add_argument("--s3_endpoint", required=True)
    parser.add_argument("--s3_access_key", required=True)
    parser.add_argument("--s3_secret_key", required=True)
    parser.add_argument("--processing_time_trigger", default="30 minutes")
    parser.add_argument("--schema_version_s3_bucket", required=True)
    parser.add_argument("--schema_version_s3_key", required=True)
    parser.add_argument("--slack_webhook_url", default=None)

    args = parser.parse_args()

    # S3 client
    s3_client = boto3.client(
        's3',
        endpoint_url=args.s3_endpoint,
        aws_access_key_id=args.s3_access_key,
        aws_secret_access_key=args.s3_secret_key
    )

    # Fetch current schema
    current_schema_str, current_version = fetch_avro_schema(
        args.schema_registry_url, args.schema_subject
    )
    print("DEBUG: Fetched Avro schema string from Schema Registry:") # 디버그 로그 추가
    print(current_schema_str)

    current_schema_hash = get_schema_hash(current_schema_str)
    spark_schema = avro_json_to_spark_schema(current_schema_str)

    # Check for schema change
    # (스키마 변경 감지 및 알림 로직은 동일하게 유지)
    hash_s3_key = args.schema_version_s3_key.replace(".version", ".hash")
    last_schema_hash = get_last_schema_hash_from_s3(
        s3_client, args.schema_version_s3_bucket, hash_s3_key
    )

    if last_schema_hash != current_schema_hash:
        msg = f"""
✨ *Avro 스키마 변경 감지!*
*Subject:* `{args.schema_subject}`
*이전 해시:* `{last_schema_hash or '없음'}`
*신규 해시:* `{current_schema_hash}`
"""
        send_slack_notification(args.slack_webhook_url, msg)
        save_schema_hash_to_s3(s3_client, args.schema_version_s3_bucket, hash_s3_key, current_schema_hash)
        schema_key = args.schema_version_s3_key.replace(".version", f"_v{current_version}.avsc")
        save_schema_to_s3(s3_client, args.schema_version_s3_bucket, schema_key, current_schema_str)
    else:
        print("✅ Avro 스키마 변경 없음")

    # SparkSession
    spark = SparkSession.builder \
        .appName("KafkaToIceberg") \
        .config(f"spark.sql.catalog.{args.iceberg_catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{args.iceberg_catalog_name}.type", "hadoop") \
        .config(f"spark.sql.catalog.{args.iceberg_catalog_name}.warehouse", args.iceberg_warehouse_path) \
        .config("spark.hadoop.fs.s3a.endpoint", args.s3_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", args.s3_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", args.s3_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

    # 원본 데이터 저장 테이블
    raw_data_table = f"{args.iceberg_catalog_name}.{args.iceberg_db_name}.{args.iceberg_table_name}"
    # 집계 데이터 저장 테이블 (예시: 비디오별 클릭 수)
    video_clicks_summary_table = f"{args.iceberg_catalog_name}.{args.iceberg_db_name}.video_clicks_summary"

    # Kafka ingestion
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.kafka_brokers) \
        .option("subscribe", args.kafka_topic) \
        .option("startingOffsets", "latest") \
        .load()

    # 원본 데이터 파싱 및 전처리
    # 1. Avro 디코딩 (PERMISSIVE 모드로 변경하여 일부 레코드 오류에 강인하게)
    decoded_df = df.select(
        from_avro(col("value"), current_schema_str, {"mode": "PERMISSIVE"}).alias("data")
    )

    # 2. 파싱 실패 레코드 필터링 (data 컬럼이 null인 경우)
    #    이러한 레코드는 별도로 로깅하거나 Dead Letter Queue(DLQ)로 보낼 수 있습니다.
    parsed_df = decoded_df.filter(col("data").isNotNull()).select("data.*")
    print("DEBUG: Schema of parsed_df AFTER from_avro and select(\"data.*\"):") # 디버그 로그 추가
    parsed_df.printSchema() 

    # 3. event_time 컬럼 추가 (타임스탬프 변환)
    #    'timestamp' 필드의 실제 데이터 타입에 따라 변환 방식이 달라질 수 있습니다.
    #    Avro 스키마에서 'timestamp'가 long (밀리초) 또는 string (ISO 형식 등)일 수 있습니다.
    expected_event_time_col = "event_time" # 사용할 컬럼명 변수화

    if "timestamp" in parsed_df.columns:
        timestamp_dtype = parsed_df.schema["timestamp"].dataType
        print(f"INFO: 'timestamp' 컬럼 발견. 타입: {timestamp_dtype}")
        if isinstance(timestamp_dtype, LongType):
            # Avro 'long' 타입 (밀리초 단위 Unix timestamp로 가정)
            parsed_df = parsed_df.withColumn(expected_event_time_col, (col("timestamp") / 1000).cast(TimestampType()))
            print(f"INFO: 'timestamp' (LongType) 필드를 밀리초 epoch로 간주하여 '{expected_event_time_col}' (TimestampType)으로 변환했습니다.")
        elif isinstance(timestamp_dtype, StringType):
            # Avro 'string' 타입
            parsed_df = parsed_df.withColumn(expected_event_time_col, to_timestamp(col("timestamp")))
            print(f"INFO: 'timestamp' (StringType) 필드를 '{expected_event_time_col}' (TimestampType)으로 변환했습니다 (to_timestamp 사용).")
        elif isinstance(timestamp_dtype, (IntegerType, DoubleType, FloatType)): # 숫자형 (초 단위 Unix timestamp로 가정)
            parsed_df = parsed_df.withColumn(expected_event_time_col, col("timestamp").cast(TimestampType()))
            print(f"INFO: 'timestamp' ({str(timestamp_dtype)}) 필드를 초단위 epoch로 간주하여 '{expected_event_time_col}' (TimestampType)으로 변환했습니다.")
        else:
            # 지원하지 않는 타입이거나, 이미 TimestampType일 수도 있습니다.
            print(f"경고: 'timestamp' 컬럼의 타입({str(timestamp_dtype)})이 예상과 다릅니다. '{expected_event_time_col}'을 null로 채웁니다.")
            parsed_df = parsed_df.withColumn(expected_event_time_col, lit(None).cast(TimestampType()))
    else:
        print(f"경고: Avro 데이터에 'timestamp' 필드가 없습니다. '{expected_event_time_col}'을 null로 채웁니다. Iceberg 테이블 스키마에서 해당 컬럼이 필수라면 문제가 발생할 수 있습니다.")
        parsed_df = parsed_df.withColumn(expected_event_time_col, lit(None).cast(TimestampType()))

    print(f"DEBUG: Schema of parsed_df AFTER attempting to add {expected_event_time_col}:") # 디버그 로그 추가
    parsed_df.printSchema()

    # 최종 확인: event_time 컬럼이 실제로 추가되었는지 확인
    if expected_event_time_col not in parsed_df.columns:
        print(f"CRITICAL ERROR: '{expected_event_time_col}' 컬럼이 parsed_df에 최종적으로 추가되지 않았습니다. 로직 점검이 시급합니다.")
        # 이 경우, 스키마 불일치로 Iceberg 쓰기 실패 가능성 매우 높음

    # 1. 원본 데이터를 Iceberg 테이블에 저장하는 스트리밍 쿼리
    raw_data_query = parsed_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", f"{args.checkpoint_location}/raw_data") \
        .toTable(raw_data_table)

    # 2. 실시간 집계: 비디오별 'content_click' 이벤트 수 집계
    #    (상태 저장(stateful) 스트리밍)
    video_click_counts_df = parsed_df \
        .filter(col("eventType") == "content_click") \
        .groupBy("videoId", "title") \
        .agg(
            count("*").alias("click_count"),
            # 집계 시점의 타임스탬프를 event_time으로 추가한다고 가정
            # 실제로는 윈도우의 시작/종료 시간을 사용하거나 다른 논리가 필요할 수 있음
            current_timestamp().alias("event_time") 
            )

    # 집계 결과를 다른 Iceberg 테이블에 저장하는 스트리밍 쿼리
    # outputMode를 "update" 또는 "complete"로 사용 (집계 유형에 따라 다름)
    # "update" 모드는 변경된 행만 업데이트 (워터마크 필요할 수 있음)
    # "complete" 모드는 전체 집계 결과를 매번 덮어씀 (상태가 크지 않을 때 유용)
    # 여기서는 간단히 "update" 모드를 사용. (Iceberg는 MERGE INTO를 통해 update 모드 지원)
    aggregated_data_query = video_click_counts_df.writeStream \
        .format("iceberg") \
        .outputMode("update") \
        .option("checkpointLocation", f"{args.checkpoint_location}/video_clicks_summary") \
        .trigger(processingTime=args.processing_time_trigger) \
        .toTable(video_clicks_summary_table)

    try:
        # 여러 스트리밍 쿼리를 동시에 실행하려면 awaitTermination()을 직접 호출하기보다
        # SparkSession.streams.awaitAnyTermination() 또는 각 쿼리를 별도 스레드에서 관리해야 할 수 있습니다.
        # 여기서는 간단히 마지막 쿼리에 대해 awaitTermination()을 호출합니다.
        # 더 견고한 관리를 위해서는 spark.streams.awaitAnyTermination() 사용을 고려하세요.
        print(f"🚀 원본 데이터 스트림 시작: {raw_data_table}")
        print(f"🚀 비디오 클릭 수 집계 스트림 시작: {video_clicks_summary_table}")
        spark.streams.awaitAnyTermination() # 모든 활성 스트림 중 하나라도 종료될 때까지 대기
    except Exception as e:
        error_message = f"""
❌ *Spark 스트리밍 쿼리 오류 발생!*
*오류 내용:* `{str(e)}`
*Iceberg 테이블 (원본):* `{raw_data_table}`
*Iceberg 테이블 (집계):* `{video_clicks_summary_table}`
*참고:* 스키마 변경으로 인한 문제일 수 있습니다.
"""
        send_slack_notification(args.slack_webhook_url, error_message)
        raise  # 에러를 다시 발생시켜 Airflow 태스크 실패 처리



if __name__ == "__main__":
    main()
