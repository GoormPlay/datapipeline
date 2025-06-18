#!/bin/bash

# config.yaml 파일의 값들을 기반으로 설정합니다.
# 실제 환경에서는 이 값들을 config.yaml에서 읽어오거나 환경 변수로 설정할 수 있습니다.

# Spark 설정 (config.yaml 참조)
SPARK_MASTER="local" # config.yaml: spark.master
# 필요한 패키지 (config.yaml: spark.packages 에서 Iceberg 및 S3 관련 패키지, Kafka는 Thrift 서버에 불필요)
# Kafka 관련 패키지는 Thrift 서버가 직접 Kafka를 읽지 않는다면 필수는 아님
SPARK_PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262" # config.yaml: spark.packages (Kafka 제외)

# Iceberg 설정 (config.yaml: iceberg 참조)
ICEBERG_CATALOG_NAME="userlogs_catalog" # config.yaml: iceberg.catalog_name
ICEBERG_WAREHOUSE_PATH="s3a://userlog-data/warehouse" # config.yaml: iceberg.warehouse_path
ICEBERG_DB_NAME="analytics" # config.yaml: iceberg.db_name (SQLAlchemy URI에 사용될 수 있음)

# S3 설정 (config.yaml: s3 참조)
S3_ENDPOINT="http://54.180.166.228:9000" # config.yaml: s3.endpoint
# S3_ACCESS_KEY와 S3_SECRET_KEY는 Thrift 서버 실행 환경에 환경 변수로 설정되어 있어야 합니다.
# 예: export S3_ACCESS_KEY="your_access_key"
# 예: export S3_SECRET_KEY="your_secret_key"
S3_PATH_STYLE_ACCESS="true" # config.yaml: s3.path_style_access
S3_CONNECTION_SSL_ENABLED="false" # config.yaml: s3.connection_ssl_enabled

# Thrift Server 포트 (기본값: 10000)
THRIFT_PORT="10000"

echo "Starting Spark Thrift Server..."

# .env 파일에서 S3 키 로드 시도 (환경 변수가 이미 설정되지 않은 경우)
ENV_FILE=".env"
if [ -f "$ENV_FILE" ]; then
  echo "INFO: .env 파일 발견. S3 접속 정보를 로드합니다."
  # S3_ACCESS_KEY 로드
  if [ -z "$S3_ACCESS_KEY" ]; then
    S3_ACCESS_KEY_FROM_ENV=$(grep -E '^S3_ACCESS_KEY=' "$ENV_FILE" | cut -d '=' -f2-)
    S3_ACCESS_KEY_FROM_ENV=$(echo "$S3_ACCESS_KEY_FROM_ENV" | sed -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//") # 따옴표 제거
    export S3_ACCESS_KEY="$S3_ACCESS_KEY_FROM_ENV"
  fi
  # S3_SECRET_KEY 로드
  if [ -z "$S3_SECRET_KEY" ]; then
    S3_SECRET_KEY_FROM_ENV=$(grep -E '^S3_SECRET_KEY=' "$ENV_FILE" | cut -d '=' -f2-)
    S3_SECRET_KEY_FROM_ENV=$(echo "$S3_SECRET_KEY_FROM_ENV" | sed -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//") # 따옴표 제거
    export S3_SECRET_KEY="$S3_SECRET_KEY_FROM_ENV"
  fi
  # SPARK_HOME 로드
  if [ -z "$SPARK_HOME" ]; then
    SPARK_HOME_FROM_ENV=$(grep -E '^SPARK_HOME=' "$ENV_FILE" | cut -d '=' -f2-)
    SPARK_HOME_FROM_ENV=$(echo "$SPARK_HOME_FROM_ENV" | sed -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//") # 따옴표 제거
    export SPARK_HOME="$SPARK_HOME_FROM_ENV"
  fi
fi

# Thrift 서버 시작 명령어
if [ -z "$SPARK_HOME" ]; then
  echo "오류: SPARK_HOME 환경 변수가 설정되지 않았습니다."
  echo "예: export SPARK_HOME=/path/to/your/spark"
  exit 1
fi

if [ -z "$S3_ACCESS_KEY" ] || [ -z "$S3_SECRET_KEY" ]; then
  echo "경고: S3_ACCESS_KEY 또는 S3_SECRET_KEY 환경 변수가 설정되지 않았습니다."
  echo "Thrift 서버가 S3에 접근하지 못할 수 있습니다."
  echo "예: export S3_ACCESS_KEY='YOUR_ACCESS_KEY'"
  echo "예: export S3_SECRET_KEY='YOUR_SECRET_KEY'"
fi

$SPARK_HOME/sbin/start-thriftserver.sh \
  --master "${SPARK_MASTER}" \
  --packages "${SPARK_PACKAGES}" \
  --hiveconf hive.server2.thrift.port="${THRIFT_PORT}" \
  --conf spark.sql.catalog.${ICEBERG_CATALOG_NAME}=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.${ICEBERG_CATALOG_NAME}.type=hadoop \
  --conf spark.sql.catalog.${ICEBERG_CATALOG_NAME}.warehouse=${ICEBERG_WAREHOUSE_PATH} \
  --conf spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT} \
  --conf spark.hadoop.fs.s3a.access.key=${S3_ACCESS_KEY} \
  --conf spark.hadoop.fs.s3a.secret.key=${S3_SECRET_KEY} \
  --conf spark.hadoop.fs.s3a.path.style.access=${S3_PATH_STYLE_ACCESS} \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=${S3_CONNECTION_SSL_ENABLED}
  # 필요한 경우 추가 Spark 설정 추가 (예: 드라이버/익스큐터 메모리 등)
echo ""
echo "Spark Thrift Server가 시작되었습니다."
echo "클라이언트에서 접속 시 사용할 수 있는 SQLAlchemy URI 예시 (PyHive 사용 시):"
echo "  hive://<THRIFT_SERVER_HOST>:${THRIFT_PORT}/${ICEBERG_DB_NAME}"
echo "  여기서 <THRIFT_SERVER_HOST>는 클라이언트 환경에 따라 localhost, host.docker.internal, 또는 Thrift 서버의 IP 주소 등이 될 수 있습니다."
