from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, count, window
import pyspark.sql.functions as func
from pyspark.sql.avro.functions import from_avro
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



# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka-to-iceberg')
logger.setLevel(logging.DEBUG)

### 스키마 레지스트리 관련 함수 ###

def fetch_avro_schema(schema_registry_url, subject):
    """스키마 레지스트리에서 최신 Avro 스키마 가져오기"""
    if SchemaRegistryClient is None:
        logger.error("confluent_kafka.schema_registry.SchemaRegistryClient를 찾을 수 없습니다. confluent-kafka 라이브러리가 설치되어 있는지 확인하세요.")
        raise ImportError("SchemaRegistryClient is not available from confluent_kafka.schema_registry")

    sr_conf = {'url': schema_registry_url}
    sr_client = SchemaRegistryClient(sr_conf)
    logger.debug(f"SchemaRegistryClient 초기화 완료. URL: {schema_registry_url}")

    try:
        logger.debug(f"스키마 레지스트리에서 '{subject}' 서브젝트의 최신 버전 스키마 가져오기 시도...")
        registered_schema = sr_client.get_latest_version(subject) # latest_version
        
        if registered_schema is None or registered_schema.schema is None or not registered_schema.schema.schema_str:
            err_msg = f"'{subject}' 서브젝트에 대한 스키마를 찾을 수 없거나 스키마 문자열이 비어 있습니다."
            logger.error(err_msg)
            raise ValueError(err_msg)

        schema_str = registered_schema.schema.schema_str
        version = registered_schema.version
        logger.info(f"스키마 레지스트리에서 스키마 성공적으로 가져옴: 버전 {version}, Subject='{subject}'")
        return schema_str, version

    except SchemaRegistryError as e:
        logger.error(f"SchemaRegistryError 발생 (서브젝트: {subject}): {e}")
        # SchemaRegistryError는 HTTP 상태 코드 등을 포함할 수 있음
        # e.g., e.http_status_code, e.message
        raise
    except Exception as e:
        logger.error(f"스키마 가져오기 중 예기치 않은 오류 발생 (서브젝트: {subject}): {e}")
        raise

def get_schema_hash(schema_str):
    """스키마 문자열의 해시값 계산"""
    schema_dict = json.loads(schema_str)
    normalized = json.dumps(schema_dict, sort_keys=True)
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()

def get_last_schema_hash_from_s3(s3_client, bucket, key):
    """S3에서 이전에 저장된 스키마 해시 가져오기"""
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        return obj['Body'].read().decode('utf-8')
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return None
        else:
            logger.error(f"S3에서 스키마 해시 가져오기 실패: {str(e)}")
            raise

def save_schema_hash_to_s3(s3_client, bucket, key, schema_hash):
    """S3에 스키마 해시 저장"""
    s3_client.put_object(Bucket=bucket, Key=key, Body=schema_hash.encode('utf-8'))
    logger.info(f"스키마 해시 S3에 저장됨: {bucket}/{key}")

def save_schema_to_s3(s3_client, bucket, key, schema_str):
    """S3에 스키마 문자열 저장"""
    s3_client.put_object(Bucket=bucket, Key=key, Body=schema_str.encode('utf-8'))
    logger.info(f"스키마 파일 S3에 저장됨: {bucket}/{key}")

def send_slack_notification(webhook_url, message):
    """Slack에 알림 전송"""
    if webhook_url:
        try:
            response = requests.post(webhook_url, json={'text': message}, timeout=10)
            if response.status_code == 200:
                logger.info("✅ Slack 알림 전송 성공")
            else:
                logger.warning(f"❌ Slack 알림 전송 실패 (상태 코드: {response.status_code})")
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Slack 알림 전송 중 예외 발생: {str(e)}")
    else:
        logger.info("⚠️ Slack Webhook URL이 없어 알림 생략됨")

### Iceberg 처리 함수 ###

def upsert_video_clicks_batch(batch_df, batch_id, video_clicks_summary_table_name):
    """
    각 마이크로배치를 Iceberg 테이블에 upsert 처리하는 함수
    """
    if batch_df.isEmpty():
        logger.info(f"🔄 배치 {batch_id}: 데이터 없음, 처리 건너뜀")
        return
    
    logger.info(f"🔄 배치 {batch_id}: {batch_df.count()} 레코드 처리 시작 (대상 테이블: {video_clicks_summary_table_name})")
    
    # 디버깅: 배치 데이터 샘플 확인
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"배치 {batch_id} 샘플 데이터:")
        batch_df.show(5, truncate=False)
    
    # Iceberg 테이블에 MERGE INTO 연산 수행 (upsert 처리)
    temp_view_name = f"updates_view_for_batch_{str(batch_id)}"
    batch_df.createOrReplaceTempView(temp_view_name)
    
    spark = batch_df.sparkSession 
    
    merge_sql = f"""
    MERGE INTO {video_clicks_summary_table_name} t
    USING {temp_view_name} s
    ON t.window_start = s.window_start AND t.videoId = s.videoId
    WHEN MATCHED THEN 
      UPDATE SET t.window_end = s.window_end, t.title = s.title, t.click_count = s.click_count
    WHEN NOT MATCHED THEN
      INSERT (window_start, window_end, videoId, title, click_count)
      VALUES (s.window_start, s.window_end, s.videoId, s.title, s.click_count)
    """
    logger.debug(f"실행할 MERGE SQL (배치 {batch_id}):\n{merge_sql}")
    
    try:
        spark.sql(merge_sql)
        logger.info(f"✅ 배치 {batch_id}: 처리 완료 (대상 테이블: {video_clicks_summary_table_name})")
    except Exception as e:
        logger.error(f"❌ 배치 {batch_id} 처리 실패: {str(e)}")
        raise
    finally:
        # 사용한 임시 뷰 삭제
        spark.catalog.dropTempView(temp_view_name)

### MAIN ###

def main():
    try:
        parser = argparse.ArgumentParser(description='Kafka에서 데이터를 읽어 Iceberg 테이블에 저장하는 ETL')
        # schema-registry
        parser.add_argument("--schema_registry_url", required=True, help='스키마 레지스트리 URL')
        parser.add_argument("--schema_subject", required=True, help='스키마 서브젝트 이름')
        # kafka
        parser.add_argument("--kafka_brokers", required=True, help='Kafka 브로커 목록 (콤마로 구분)')
        parser.add_argument("--kafka_topic", required=True, help='Kafka 토픽 이름')
        # icebrg
        parser.add_argument("--iceberg_catalog_name", required=True, help='Iceberg 카탈로그 이름')
        parser.add_argument("--iceberg_warehouse_path", required=True, help='Iceberg 웨어하우스 경로')
        parser.add_argument("--iceberg_db_name", required=True, help='Iceberg 데이터베이스 이름')
        parser.add_argument("--iceberg_table_name", required=True, help='원본 데이터 저장할 테이블 이름')
        # minIO
        parser.add_argument("--checkpoint_location", required=True, help='체크포인트 위치')
        parser.add_argument("--s3_endpoint", required=True, help='S3 엔드포인트')
        parser.add_argument("--s3_access_key", required=True, help='S3 액세스 키')
        parser.add_argument("--s3_secret_key", required=True, help='S3 시크릿 키')
        parser.add_argument("--s3_region", required=True, help='S3 리전')
        # monitoring
        parser.add_argument("--processing_time_trigger", default="30 seconds", help='처리 시간 트리거 (예: "30 seconds")')
        parser.add_argument("--schema_version_s3_bucket", required=True, help='스키마 버전 저장할 S3 버킷')
        parser.add_argument("--schema_version_s3_key", required=True, help='스키마 버전 저장할 S3 키')
        parser.add_argument("--slack_webhook_url", default=None, help='Slack 웹훅 URL')

        args = parser.parse_args()
        logger.info("입력 인수 파싱 완료")

        # S3 클라이언트 생성
        s3_client = boto3.client(
            's3',
            endpoint_url=args.s3_endpoint,
            aws_access_key_id=args.s3_access_key,
            aws_secret_access_key=args.s3_secret_key
        )
        logger.info(f"S3 클라이언트 생성 완료 (엔드포인트: {args.s3_endpoint})")

        # 스키마 레지스트리에서 현재 스키마 가져오기
        try:
            current_schema_str, current_version = fetch_avro_schema(
                args.schema_registry_url, args.schema_subject
            )
        except Exception as e: # fetch_avro_schema 내부에서 이미 로깅 및 예외 발생
            error_message = f"CRITICAL ERROR: 스키마 가져오기 실패, 스크립트 중단. 원인: {e}"
            logger.error(error_message)
            send_slack_notification(args.slack_webhook_url, error_message)
            raise
        
        if not current_schema_str:
            # 이 경우는 fetch_avro_schema 내부의 예외 처리로 인해 발생하기 어려움
            error_message = f"CRITICAL ERROR: 스키마 레지스트리에서 가져온 Avro 스키마가 비어있습니다 (URL: {args.schema_registry_url}, Subject: {args.schema_subject}). 진행할 수 없습니다."
            logger.error(error_message)
            send_slack_notification(args.slack_webhook_url, error_message)
            raise ValueError(error_message)
        
        logger.debug(f"가져온 스키마 문자열 (일부): '{current_schema_str[:200]}...'")

        # 스키마 해시 계산 및 S3에 저장된 이전 해시와 비교
        current_schema_hash = get_schema_hash(current_schema_str) # current_schema_str이 유효할 때만 호출됨
        logger.info(f"현재 스키마 해시 계산 완료: {current_schema_hash}")

        hash_s3_key = args.schema_version_s3_key.replace(".version", ".hash")
        last_schema_hash = get_last_schema_hash_from_s3(
            s3_client, args.schema_version_s3_bucket, hash_s3_key
        )

        # 스키마 변경 감지 및 알림
        if last_schema_hash != current_schema_hash:
            msg = f"""
                ✨ *Avro 스키마 변경 감지!*
                *Subject:* `{args.schema_subject}`
                *이전 해시:* `{last_schema_hash or '없음'}`
                *신규 해시:* `{current_schema_hash}`
                *버전:* `{current_version}`
                """
            logger.info("Avro 스키마 변경 감지됨!")
            send_slack_notification(args.slack_webhook_url, msg)
            save_schema_hash_to_s3(s3_client, args.schema_version_s3_bucket, hash_s3_key, current_schema_hash)
            schema_key = args.schema_version_s3_key.replace(".version", f"_v{current_version}.avsc")
            save_schema_to_s3(s3_client, args.schema_version_s3_bucket, schema_key, current_schema_str)
        else:
            logger.info("Avro 스키마 변경 없음")

        spark = None
        active_queries = []

        # SparkSession 설정 - iceberge.type 명시 에러
        spark = SparkSession.builder \
        .appName("KafkaToIcebergETL") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config(f"spark.sql.catalog.{args.iceberg_catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{args.iceberg_catalog_name}.type", "hadoop") \
        .config(f"spark.sql.catalog.{args.iceberg_catalog_name}.warehouse", args.iceberg_warehouse_path) \
        .config("spark.hadoop.fs.s3a.endpoint", args.s3_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", args.s3_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", args.s3_secret_key) \
        .config("spark.hadoop.fs.s3a.region", args.s3_region) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()
            

        logger.info("SparkSession 생성 완료")

        # Iceberg 테이블 이름 정의
        raw_data_table = f"{args.iceberg_catalog_name}.{args.iceberg_db_name}.{args.iceberg_table_name}"
        video_clicks_summary_table = f"{args.iceberg_catalog_name}.{args.iceberg_db_name}.video_clicks_summary"

        # 데이터베이스가 존재하지 않으면 생성
        logger.info(f"Ensuring database {args.iceberg_catalog_name}.{args.iceberg_db_name} exists...")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.iceberg_catalog_name}.{args.iceberg_db_name}")
        logger.info(f"Database {args.iceberg_catalog_name}.{args.iceberg_db_name} ensured.")


        # Kafka에서 데이터 읽기
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", args.kafka_brokers) \
            .option("subscribe", args.kafka_topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        logger.info(f"Kafka 스트리밍 리더 설정 완료 (토픽: {args.kafka_topic})")

        # Avro 디코딩 (Confluent 와이어 포맷 처리)
        # Kafka 'value' 컬럼에서 5바이트 헤더(Magic Byte + Schema ID)를 제거합니다.
        # from_avro 함수에 스키마 문자열을 직접 제공할 때는 순수 Avro 바이너리 데이터만 전달해야 합니다.
        fixed_value_df = df.withColumn("fixedValue", func.expr("substring(value, 6, length(value)-5)"))

        try:
            fromAvroOptions = {"mode":"PERMISSIVE"}
            from_avro_df = fixed_value_df.select(
                from_avro(col("fixedValue"), current_schema_str, fromAvroOptions).alias("data")
            )

            decoded_df = from_avro_df.select("data.*")
            
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Schema of decoded_df (after from_avro and select data.*):")
                decoded_df.printSchema()
            logger.info("Avro 디코딩 설정 완료")
        except Exception as e:
            error_message = f"Avro 디코딩 설정 중 오류: {str(e)}"
            logger.error(error_message)
            send_slack_notification(args.slack_webhook_url, error_message)
            raise

        # 디버깅: 변환된 데이터 구조 확인
        debug_decoded_query = None
        if logger.isEnabledFor(logging.DEBUG):
            # decoded_df (from_avro 직후, data.* 펼친 상태)를 콘솔에 출력
            debug_decoded_query = decoded_df.writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", "false") \
                .option("numRows", 5) \
                .start()
            logger.debug("디버그 스트림 시작됨 (콘솔 출력)")
            active_queries.append(debug_decoded_query)

        # event_timestamp 컬럼 생성 (Iceberg 스키마의 event_timestamp와 매칭)
        # Avro 스키마의 'timestamp' (long, milliseconds)를 Spark TimestampType으로 변환
        # 이 컬럼은 user_logs 저장 및 집계에 사용됨
        if "timestamp" in decoded_df.columns:
            df_with_event_timestamp = decoded_df.withColumn(
                "event_timestamp", 
                (col("timestamp") / 1000).cast(TimestampType())
            )
            logger.info("'event_timestamp' 컬럼 생성 완료 (from 'timestamp')")
        else: # pragma: no cover
            logger.warning("Avro 데이터에 'timestamp' 필드가 없어 'event_timestamp'를 null로 채웁니다.")
            df_with_event_timestamp = decoded_df.withColumn("event_timestamp", func.lit(None).cast(TimestampType()))
        
        if logger.isEnabledFor(logging.DEBUG): # pragma: no cover
            logger.debug("Schema of df_with_event_timestamp:")
            df_with_event_timestamp.printSchema()

        # Define the target Iceberg schema for user_logs table
        # This schema should match the intended structure of your 'user_logs' Iceberg table.
        user_logs_iceberg_schema = StructType([
            StructField("videoId", StringType(), True),
            StructField("title", StringType(), True),
            StructField("userId", StringType(), True),
            StructField("timestamp", LongType(), True), # Original timestamp from Avro
            StructField("eventType", StringType(), True),
            StructField("page", StringType(), True),
            StructField("liked", BooleanType(), True),
            StructField("review", StringType(), True),
            StructField("rating", IntegerType(), True),
            StructField("genre", ArrayType(StringType(), True), True),
            StructField("recMovieList", StringType(), True),
            StructField("event_timestamp", TimestampType(), True) # Derived event timestamp
        ])
        logger.info(f"Defined target Iceberg schema for 'user_logs': {user_logs_iceberg_schema.simpleString()}")

        # user_logs_iceberg_schema를 사용하여 user_logs 테이블이 존재하지 않으면 생성
        # (위에서 언급된 DDL 실행 로직을 여기에 위치)
        logger.info(f"Ensuring table {raw_data_table} exists with the correct schema...")
        columns_ddl_parts = []
        for field in user_logs_iceberg_schema.fields:
            columns_ddl_parts.append(f"`{field.name}` {field.dataType.simpleString()}")
        columns_ddl = ", ".join(columns_ddl_parts)

        ######################################################################3
        
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {raw_data_table} ({columns_ddl}) USING iceberg"
        # 필요시 파티셔닝 추가: e.g., PARTITIONED BY (days(event_timestamp))
        spark.sql(create_table_sql)
        logger.info(f"Table {raw_data_table} ensured.")
        try:
            spark.catalog.refreshTable(raw_data_table)
            logger.info(f"Refreshed catalog for table {raw_data_table}")
        except Exception as e_refresh:
            # If the table truly didn't exist before the DDL, refreshTable might fail.
            logger.warning(f"Could not refresh table {raw_data_table} in catalog (this might be okay if it was just created): {e_refresh}")

        # 1. 원본 데이터를 Iceberg user_logs 테이블에 저장 (느슨한 결합)
        raw_data_to_iceberg_query = None
        logger.info("Iceberg 'user_logs' 테이블 저장 스트림 설정 시작...")
        select_exprs_for_user_logs = []
        for field in user_logs_iceberg_schema.fields: # user_logs_iceberg_schema는 이전에 정의되어 있어야 함
            if field.name in df_with_event_timestamp.columns:
                select_exprs_for_user_logs.append(col(field.name).cast(field.dataType).alias(field.name))
            else: # pragma: no cover
                logger.warning(f"Iceberg 스키마 필드 '{field.name}'가 입력 데이터에 없습니다. Null로 채웁니다.")
                select_exprs_for_user_logs.append(func.lit(None).cast(field.dataType).alias(field.name))
        
        transformed_for_user_logs_df = df_with_event_timestamp.select(select_exprs_for_user_logs)
        
        if logger.isEnabledFor(logging.DEBUG): # pragma: no cover
            logger.debug("Schema of transformed_for_user_logs_df (to be written to user_logs):")
            transformed_for_user_logs_df.printSchema()

        try:
            raw_data_to_iceberg_query = transformed_for_user_logs_df.writeStream \
                .format("iceberg") \
                .outputMode("append") \
                .option("checkpointLocation", f"{args.checkpoint_location}/raw_data") \
                .toTable(raw_data_table)
            logger.info(f"원본 데이터 Iceberg 테이블 스트림 시작됨: {raw_data_table}")
            active_queries.append(raw_data_to_iceberg_query)    
    
        except Exception as e:
            error_message = f"원본 데이터 Iceberg 저장 설정 중 오류: {str(e)}"
            logger.error(error_message)
            send_slack_notification(args.slack_webhook_url, error_message)
            raise

        # 2. 비디오 클릭 집계
        aggregated_data_query = None # 초기화
        try:
            # 집계는 event_timestamp 컬럼이 유효할 때만 수행
            if "event_timestamp" in df_with_event_timestamp.columns and \
               isinstance(df_with_event_timestamp.schema["event_timestamp"].dataType, TimestampType):
                logger.info("'event_timestamp'을 사용하여 윈도우 집계를 수행합니다.")
                
                df_for_aggregation = df_with_event_timestamp.select("videoId", "title", "eventType", "event_timestamp")
                video_click_counts_df = df_for_aggregation \
                .filter(col("eventType") == "content_click") \
                .withWatermark("event_timestamp", "5 minutes") \
                .groupBy(
                    window(col("event_timestamp"), args.processing_time_trigger).alias("time_window"),
                    col("videoId"),
                    col("title")
                ) \
                .agg(count("*").alias("click_count")) \
                .select(
                    col("time_window.start").alias("window_start"),
                    col("time_window.end").alias("window_end"),
                    col("videoId"),
                    col("title"),
                    col("click_count")
                )
                
                if logger.isEnabledFor(logging.DEBUG): # pragma: no cover
                    logger.debug("Schema of video_click_counts_df (for aggregation):")
                    video_click_counts_df.printSchema()

                aggregated_data_query = video_click_counts_df.writeStream \
                    .foreachBatch(lambda df_batch, batch_id: upsert_video_clicks_batch(df_batch, batch_id, video_clicks_summary_table)) \
                    .option("checkpointLocation", f"{args.checkpoint_location}/video_clicks_summary_windowed") \
                    .outputMode("update") \
                    .trigger(processingTime=args.processing_time_trigger) \
                    .start()
                logger.info(f"Iceberg 'video_clicks_summary' 테이블 저장 스트림 (foreachBatch) 정의 완료: {video_clicks_summary_table}")
                active_queries.append(aggregated_data_query)
            else: # pragma: no cover
                logger.warning("'event_timestamp' 컬럼이 없거나 TimestampType이 아니므로, 윈도우 집계를 건너뜁니다.")
        except Exception as e:
            error_message = f"비디오 클릭 집계 설정 중 오류: {str(e)}"
            logger.error(error_message)
            send_slack_notification(args.slack_webhook_url, error_message)
            # 이 오류는 치명적이지 않을 수 있으므로 원본 데이터 스트림은 유지
            logger.warning("비디오 클릭 집계 스트림은 시작되지 않았지만, 원본 데이터 스트림은 계속됩니다.")

        # 모든 쿼리가 종료될 때까지 대기
        if active_queries:
            logger.info(f"{len(active_queries)}개의 활성 스트림 쿼리가 실행 중입니다. 종료 대기 중...")
            for query in active_queries:
                if query and query.isActive:
                    query.awaitTermination()

    except Exception as e:
        error_message = f"애플리케이션 실행 중 예기치 않은 오류: {str(e)}"
        logger.error(error_message, exc_info=True)
        if 'args' in locals() and hasattr(args, 'slack_webhook_url'):
            send_slack_notification(args.slack_webhook_url, error_message)
        
        # 활성화된 스트림 쿼리 종료
        if 'active_queries' in locals() and active_queries:
            for query in active_queries:
                try:
                    if query and query.isActive:
                        query.stop()
                        logger.info("스트림 쿼리가 정상적으로 종료되었습니다.")
                except Exception as stop_error:
                    logger.error(f"쿼리 종료 중 오류: {stop_error}")
        
        # SparkSession 종료 - 인스턴스 체크 후 종료
        if spark is not None and not spark._jsc.sc().isStopped():
            try:
                spark.stop()
                logger.info("SparkSession이 정상적으로 종료되었습니다.")
            except Exception as stop_error:
                logger.error(f"SparkSession 종료 중 오류: {stop_error}")
        
        # 명시적으로 현재 예외 다시 발생
        sys.exit(1)  # 프로그램 종료 with error code
        
    finally:
        # 정상 종료 시에도 SparkSession과 쿼리 정리
        # 활성화된 스트림 쿼리 종료
        if 'active_queries' in locals() and active_queries:
            for query in active_queries:
                try:
                    if query and query.isActive:
                        query.stop()
                        logger.info("스트림 쿼리가 정상적으로 종료되었습니다.")
                except Exception as stop_error:
                    logger.error(f"쿼리 종료 중 오류: {stop_error}")
        
        # SparkSession 종료 - 인스턴스 체크 후 종료
        if spark is not None and not spark._jsc.sc().isStopped():
            try:
                spark.stop()
                logger.info("SparkSession이 정상적으로 종료되었습니다.")
            except Exception as stop_error:
                logger.error(f"SparkSession 종료 중 오류: {stop_error}")

if __name__ == "__main__":
    main()