#!/bin/bash

# Spark 설정 (config.yaml 참조)
SPARK_MASTER="local" # config.yaml: spark.master
# 필요한 패키지
SPARK_PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262"

# Iceberg 설정 (config.yaml: iceberg 참조)
ICEBERG_CATALOG_NAME="userlogs_catalog" # config.yaml: iceberg.catalog_name
ICEBERG_WAREHOUSE_PATH="s3a://userlog-data/warehouse" # config.yaml: iceberg.warehouse_path
ICEBERG_DB_NAME="analytics" # config.yaml: iceberg.db_name (SQLAlchemy URI에 사용될 수 있음)

# S3 설정 (config.yaml: s3 참조)
S3_ENDPOINT="http://54.180.166.228:9000" # config.yaml: s3.endpoint
S3_ACCESS_KEY="adminminio"
S3_SECRET_KEY="adminminio"
S3_PATH_STYLE_ACCESS="true" # config.yaml: s3.path_style_access
S3_CONNECTION_SSL_ENABLED="false" # config.yaml: s3.connection_ssl_enabled

# Thrift Server 포트 (기본값: 10000)
THRIFT_PORT="10000"

echo "Starting Spark Thrift Server..."
$SPARK_HOME/sbin/start-thriftserver.sh \
  --master "${SPARK_MASTER}" \
  --packages "${SPARK_PACKAGES}" \
  --hiveconf hive.server2.thrift.port="${THRIFT_PORT}" \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.${ICEBERG_CATALOG_NAME}=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.${ICEBERG_CATALOG_NAME}.type=hadoop \
  --conf spark.sql.catalog.${ICEBERG_CATALOG_NAME}.warehouse=${ICEBERG_WAREHOUSE_PATH} \
  --conf spark.sql.catalog.spark_catalog.warehouse=${ICEBERG_WAREHOUSE_PATH} \
  --conf spark.sql.defaultCatalog=${ICEBERG_CATALOG_NAME} \
  --conf spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT} \
  --conf spark.hadoop.fs.s3a.access.key=${S3_ACCESS_KEY} \
  --conf spark.hadoop.fs.s3a.secret.key=${S3_SECRET_KEY} \
  --conf spark.hadoop.fs.s3a.path.style.access=${S3_PATH_STYLE_ACCESS} \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=${S3_CONNECTION_SSL_ENABLED}

echo ""
echo "Spark Thrift Server가 시작되었습니다."
echo "클라이언트에서 접속 시 사용할 수 있는 SQLAlchemy URI 예시 (PyHive 사용 시):"
echo "  hive://<THRIFT_SERVER_HOST>:${THRIFT_PORT}/${ICEBERG_DB_NAME}"
echo "  여기서 <THRIFT_SERVER_HOST>는 클라이언트 환경에 따라 localhost, host.docker.internal, 또는 Thrift 서버의 IP 주소 등이 될 수 있습니다."