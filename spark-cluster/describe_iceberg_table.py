from pyspark.sql import SparkSession

# --------------------------------------------------------------------------------
# 사용자 설정 값: MinIO 및 Iceberg 테이블 정보를 여기에 입력하세요.
# --------------------------------------------------------------------------------
MINIO_ENDPOINT = "http://54.180.166.228:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

ICEBERG_CATALOG_NAME = "userlogs_catalog"
ICEBERG_WAREHOUSE_PATH = "s3a://userlog-data/warehouse"

# 스키마를 확인할 데이터베이스 이름과 테이블 이름
ICEBERG_DB_NAME = "analytics"
ICEBERG_TABLE_NAME = "user_logs"
# --------------------------------------------------------------------------------
# Spark 버전 및 Iceberg 버전에 맞는 패키지를 지정하세요.
# 쉼표 뒤에 공백이 없도록 하고, 문자열 끝에 쉼표가 없도록 수정했습니다.
ICEBERG_PACKAGES = "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262"

def main():
    print("로컬 Spark 세션 초기화 중...")
    try:
        spark = SparkSession.builder \
            .appName("DescribeIcebergTableLocal") \
            .master("local[*]") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.jars.packages", ICEBERG_PACKAGES) \
            .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog") \
            .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.type", "hadoop") \
            .config(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.warehouse", ICEBERG_WAREHOUSE_PATH) \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .getOrCreate()

        print("Spark 세션 생성 완료.")
        
        table_identifier = f"{ICEBERG_CATALOG_NAME}.{ICEBERG_DB_NAME}.{ICEBERG_TABLE_NAME}"

        # 데이터베이스 목록 확인
        print("\n사용 가능한 데이터베이스 목록:")
        databases_df = spark.sql("SHOW DATABASES")
        databases_df.show(truncate=False)
        
        # 데이터베이스가 존재하는지 확인
        db_exists = any(row['namespace'] == f"{ICEBERG_CATALOG_NAME}.{ICEBERG_DB_NAME}" for row in databases_df.collect())
        
        if db_exists:
            # 테이블 목록 확인
            print(f"\n{ICEBERG_CATALOG_NAME}.{ICEBERG_DB_NAME} 데이터베이스의 테이블 목록:")
            tables_df = spark.sql(f"SHOW TABLES IN {ICEBERG_CATALOG_NAME}.{ICEBERG_DB_NAME}")
            tables_df.show(truncate=False)
            
            # 테이블이 존재하는지 확인
            table_exists = any(row['tableName'] == ICEBERG_TABLE_NAME for row in tables_df.collect())
                
            if table_exists:
                print(f"\n테이블 스키마 조회 중: {table_identifier}")
                # 테이블 스키마 출력
                try:
                    # 먼저 테이블 스키마 조회
                    schema_df = spark.sql(f"DESCRIBE TABLE {table_identifier}")
                    print("\n테이블 스키마:")
                    schema_df.show(truncate=False)
                except Exception as schema_err:
                    print(f"스키마 조회 오류: {schema_err}")
                    print("대안적인 방법으로 테이블 스키마를 조회합니다.")
                    table_df = spark.table(table_identifier)
                    print("테이블 스키마:")
                    table_df.printSchema()
                
                # 테이블 데이터 샘플 조회
                try:
                    print("\n테이블 데이터 샘플 (최대 5개 행):")
                    sample_df = spark.sql(f"SELECT * FROM {table_identifier} LIMIT 5")
                    sample_df.show(truncate=False)
                except Exception as sample_err:
                    print(f"데이터 샘플 조회 오류: {sample_err}")
                
                # 테이블 통계 정보
                try:
                    print("\n테이블 통계 정보:")
                    stats_df = spark.table(table_identifier).describe()
                    stats_df.show()
                except Exception as stats_err:
                    print(f"통계 정보 조회 오류: {stats_err}")
                
                # Iceberg 세부 정보 조회
                try:
                    print("\nIceberg 테이블 세부 정보:")
                    detail_df = spark.sql(f"DESCRIBE DETAIL {table_identifier}")
                    detail_df.show(truncate=False)
                except Exception as detail_err:
                    print(f"테이블 세부 정보 조회 오류: {detail_err}")
                
                # Iceberg 히스토리 조회
                try:
                    print("\nIceberg 테이블 히스토리:")
                    history_df = spark.sql(f"SELECT * FROM {table_identifier}.history")
                    history_df.show(truncate=False)
                except Exception as history_err:
                    print(f"테이블 히스토리 조회 오류: {history_err}")
            else:
                print(f"\n오류: 데이터베이스 {ICEBERG_CATALOG_NAME}.{ICEBERG_DB_NAME}에 테이블 {ICEBERG_TABLE_NAME}이 존재하지 않습니다.")
        else:
            print(f"\n오류: 데이터베이스 {ICEBERG_CATALOG_NAME}.{ICEBERG_DB_NAME}가 존재하지 않습니다.")

    except Exception as e:
        print(f"오류 발생: {e}")
        print("다음 사항을 확인해보세요:")
        print(f"  1. MinIO 서버 ({MINIO_ENDPOINT})가 실행 중이고 로컬에서 접근 가능한가요?")
        print(f"  2. MinIO Access Key/Secret Key가 정확한가요?")
        print(f"  3. Iceberg 카탈로그 이름, 웨어하우스 경로, DB 이름, 테이블 이름이 정확한가요?")
        print(f"  4. 지정된 테이블 '{table_identifier}'이 실제로 MinIO에 존재하나요?")
        print(f"  5. `ICEBERG_PACKAGES`에 지정된 Spark, Iceberg, Hadoop-AWS 버전이 호환되나요?")
        print("\n디버깅 정보:")
        if 'spark' in locals():
            print("  - Spark 버전:", spark.version)
            print("  - Iceberg 설정:", spark.conf.get(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}"))
            print("  - S3 엔드포인트:", spark.conf.get("spark.hadoop.fs.s3a.endpoint"))
    finally:
        if 'spark' in locals() and spark is not None:
            spark.stop()
            print("Spark 세션 종료.")

if __name__ == "__main__":
    main()