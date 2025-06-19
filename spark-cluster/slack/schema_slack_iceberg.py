from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, count, window, when, current_timestamp
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import *
import argparse
import requests
import hashlib
import json
import boto3
import logging
from botocore.exceptions import ClientError


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
    url = f"{schema_registry_url}/subjects/{subject}/versions/latest"
    logger.debug(f"스키마 레지스트리에서 스키마 가져오기 시도: URL='{url}'")
    try:
        response = requests.get(url, timeout=10) # 타임아웃 설정
        response.raise_for_status()
        schema_data = response.json()
        if 'schema' not in schema_data or not schema_data['schema']:
            err_msg = "스키마 레지스트리 응답에 'schema' 필드가 없거나 비어 있습니다."
            logger.error(err_msg)
            raise ValueError(err_msg)
        if 'version' not in schema_data:
            err_msg = "스키마 레지스트리 응답에 'version' 필드가 없습니다."
            logger.error(err_msg)
            raise ValueError(err_msg)
        logger.info(f"스키마 레지스트리에서 스키마 성공적으로 가져옴: 버전 {schema_data['version']}, Subject='{subject}'")
        return schema_data['schema'], schema_data['version']
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"스키마 가져오기 중 HTTP 오류 발생: {http_err} - URL: {url} - 응답: {http_err.response.text if http_err.response else '응답 없음'}")
        raise
    except requests.exceptions.RequestException as req_err:
        logger.error(f"스키마 가져오기 중 요청 오류 발생: {req_err} - URL: {url}")
        raise
    except ValueError as val_err: # JSON 파싱 또는 응답 형식 오류
        logger.error(f"스키마 응답 파싱 중 오류 또는 잘못된 형식: {val_err} - URL: {url}")
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

def process_video_clicks_batch(batch_df, batch_id, video_clicks_summary_table_name):
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

        logger.info("SparkSession 생성 완료")

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

        logger.info(f"Kafka 스트리밍 리더 설정 완료 (토픽: {args.kafka_topic})")

        # Avro 디코딩
        try:
            decoded_df = df.select(
                # Confluent 와이어 포맷 처리를 위해 Schema Registry URL 사용
                from_avro(col("value"), None, {"mode": "PERMISSIVE", "schema.registry.url": args.schema_registry_url}).alias("data")
            )
            logger.info("Avro 디코딩 설정 완료")
        except Exception as e:
            error_message = f"Avro 디코딩 설정 중 오류: {str(e)}"
            logger.error(error_message)
            send_slack_notification(args.slack_webhook_url, error_message)
            raise

        # 데이터 파싱: Avro에서 디코딩된 데이터를 명시적으로 Iceberg 테이블 형식에 맞게 변환
        # 스키마 변경에 대응하기 위해 명시적인 필드 선택 및 매핑
        try:
            # 필요한 필드 명시적 추출 (Avro 스키마와 Iceberg 테이블 간 매핑)
            parsed_df = decoded_df \
                .filter(col("data").isNotNull()) \
                .select(
                    # 필드명을 명시적으로 지정하여 매핑
                    # Avro 스키마와 Iceberg 스키마 간의 중간 변환 계층 역할
                    col("data.eventType").alias("eventType"),
                    col("data.videoId").alias("videoId"), 
                    col("data.title").alias("title"),
                    col("data.userId").alias("userId"),
                    # 여기에 필요한 다른 필드 추가
                    
                    # 타임스탬프 필드 특별 처리
                    # 타입에 따라 적절히 변환
                    when(col("data.timestamp").isNotNull(), 
                         when(col("data.timestamp").cast("string").like("%:%"), 
                              to_timestamp(col("data.timestamp")))
                         .otherwise((col("data.timestamp") / 1000).cast("timestamp")))
                    .otherwise(current_timestamp())
                    .alias("event_time")
                )
            logger.info("데이터 파싱 및 변환 설정 완료")
        except Exception as e:
            error_message = f"데이터 파싱 및 변환 설정 중 오류: {str(e)}"
            logger.error(error_message)
            send_slack_notification(args.slack_webhook_url, error_message)
            raise

        # 디버깅: 변환된 데이터 구조 확인
        if logger.isEnabledFor(logging.DEBUG):
            debug_query = parsed_df.writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", "false") \
                .option("numRows", 5) \
                .start()
            logger.debug("디버그 스트림 시작됨 (콘솔 출력)")

        # 1. 원본 데이터를 Iceberg 테이블에 저장
        try:
            raw_data_query = parsed_df.writeStream \
                .format("iceberg") \
                .outputMode("append") \
                .option("checkpointLocation", f"{args.checkpoint_location}/raw_data") \
                .toTable(raw_data_table)
            logger.info(f"원본 데이터 Iceberg 테이블 스트림 시작됨: {raw_data_table}")
        except Exception as e:
            error_message = f"원본 데이터 Iceberg 저장 설정 중 오류: {str(e)}"
            logger.error(error_message)
            send_slack_notification(args.slack_webhook_url, error_message)
            raise

        # 2. 비디오 클릭 집계
        try:
            # content_click 이벤트만 필터링
            video_click_counts_df = parsed_df \
                .filter(col("eventType") == "content_click") \
                .withWatermark("event_time", "10 minutes") \
                .groupBy(
                    window(col("event_time"), "1 hour").alias("time_window"),
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

            # foreachBatch를 사용하여 집계 결과 처리
            aggregated_data_query = video_click_counts_df.writeStream \
                .foreachBatch(lambda df, id: process_video_clicks_batch(df, id, video_clicks_summary_table)) \
                .option("checkpointLocation", f"{args.checkpoint_location}/video_clicks_summary") \
                .outputMode("update") \
                .trigger(processingTime=args.processing_time_trigger) \
                .start()
                
            logger.info(f"비디오 클릭 집계 스트림 시작됨: {video_clicks_summary_table}")
        except Exception as e:
            error_message = f"비디오 클릭 집계 설정 중 오류: {str(e)}"
            logger.error(error_message)
            send_slack_notification(args.slack_webhook_url, error_message)
            # 이 오류는 치명적이지 않을 수 있으므로 원본 데이터 스트림은 유지
            logger.warning("비디오 클릭 집계 스트림은 시작되지 않았지만, 원본 데이터 스트림은 계속됩니다.")

        # 스트림 실행 상태 모니터링
        try:
            logger.info("모든 스트림 시작됨, 종료 대기 중...")
            spark.streams.awaitAnyTermination()
        except Exception as e:
            error_message = f"스트림 실행 중 오류 발생: {str(e)}"
            logger.error(error_message)
            send_slack_notification(args.slack_webhook_url, error_message)
            raise
        finally:
            logger.info("애플리케이션 종료...")
            # 필요하면 리소스 정리 코드 추가

    except Exception as e:
        error_message = f"애플리케이션 실행 중 예기치 않은 오류: {str(e)}"
        logger.error(error_message, exc_info=True)
        send_slack_notification(args.slack_webhook_url, error_message)
        raise

if __name__ == "__main__":
    main()