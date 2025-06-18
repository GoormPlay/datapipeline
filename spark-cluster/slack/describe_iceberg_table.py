from pyspark.sql import SparkSession

# --------------------------------------------------------------------------------
# 사용자 설정 값: MinIO 및 Iceberg 테이블 정보를 여기에 입력하세요.
# 이 값들은 보통 schema_slack_iceberg.py를 실행할 때 사용되는 config.yaml 파일이나
# run_spark_job.py 스크립트에 전달되는 인자에서 찾을 수 있습니다.
# --------------------------------------------------------------------------------
MINIO_ENDPOINT = "http://54.180.166.228:9000"  # 예: "http://43.202.164.175:9000"
MINIO_ACCESS_KEY = "minioadmin"      # 예: "minioadmin"
MINIO_SECRET_KEY = "minioadmin"      # 예: "minioadmin"

ICEBERG_CATALOG_NAME = "userlogs_catalog" # config.yaml의 iceberg.catalog_name과 일치시킴
ICEBERG_WAREHOUSE_PATH = f"s3a://userlog-data/warehouse" # 예: "s3a://userlog-data/iceberg_warehouse" (config.yaml의 iceberg.warehouse_path)

# 스키마를 확인할 데이터베이스 이름과 테이블 이름
ICEBERG_DB_NAME = "analytics"                 # 예: "userlog_db" (config.yaml의 iceberg.db_name)
ICEBERG_TABLE_NAME = "user_logs"           # 예: "user_activity_logs" (스키마를 확인하려는 테이블)
# --------------------------------------------------------------------------------
# Spark 버전 및 Iceberg 버전에 맞는 패키지를 지정하세요.
# 예시: Spark 3.4, Iceberg 1.4.2, Hadoop-AWS 3.3.4
# schema_slack_iceberg.py를 실행하는 run_spark_job.py의 config.yaml에 있는
# spark.packages 설정을 참고할 수 있습니다.
# 만약 config.yaml에 org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2 와 같은 형식으로 있다면 그대로 사용합니다.
# hadoop-aws 패키지도 필요합니다. config.yaml과 일치하도록 수정합니다.
ICEBERG_PACKAGES = "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262"
# 참고: hadoop-aws 버전(3.3.2)과 호환되는 aws sdk bundle 버전(1.12.262)을 사용합니다.
# Spark 3.3.x 이하는 com.amazonaws:aws-java-sdk-bundle:1.12.x 대를 사용 할 수 있습니다.

def main():
    print("로컬 Spark 세션 초기화 중...")
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
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()

    print("Spark 세션 생성 완료.")
    
    table_identifier = f"{ICEBERG_CATALOG_NAME}.{ICEBERG_DB_NAME}.{ICEBERG_TABLE_NAME}"

    try:
        print(f"\n테이블 스키마 조회 중: {table_identifier}")
        spark.sql(f"DESCRIBE TABLE {table_identifier}").show(truncate=False)

        print(f"\n테이블 상세 정보 (FORMATTED) 조회 중: {table_identifier}")
        # FORMATTED는 더 많은 정보를 보여주므로, row 수를 늘려서 확인 (기본은 20줄)
        spark.sql(f"DESCRIBE FORMATTED {table_identifier}").show(n=100, truncate=False)

    except Exception as e:
        print(f"오류 발생: {e}")
        print("다음 사항을 확인해보세요:")
        print(f"  1. MinIO 서버 ({MINIO_ENDPOINT})가 실행 중이고 로컬에서 접근 가능한가요?")
        print(f"  2. MinIO Access Key/Secret Key가 정확한가요?")
        print(f"  3. Iceberg 카탈로그 이름, 웨어하우스 경로, DB 이름, 테이블 이름이 정확한가요?")
        print(f"  4. 지정된 테이블 '{table_identifier}'이 실제로 MinIO에 존재하나요?")
        print(f"  5. `ICEBERG_PACKAGES`에 지정된 Spark, Iceberg, Hadoop-AWS 버전이 호환되나요?")


    spark.stop()
    print("Spark 세션 종료.")

if __name__ == "__main__":
    main()
