#!/bin/bash

SPARK_HOME=/Users/jeongmieun/app/spark  # 실제 Spark 경로로 수정하세요
SPARK_VERSION="3.5.5"
SCALA_VERSION="2.12"

# Iceberg 및 필요한 의존성 JAR 버전 설정 (Spark 3.5.5와 호환되는 버전)
ICEBERG_VERSION="1.5.0"  # Spark 3.5.x와 호환되는 Iceberg 버전
AWS_SDK_VERSION="2.20.160"  # 최신 안정 버전
NESSIE_VERSION="0.76.1"  # Iceberg 1.5.0과 호환되는 Nessie 버전

# 필요한 JAR 파일들
JARS=(
  "/path/to/iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar"
  "/path/to/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"
  "/path/to/nessie-spark-extensions-${SPARK_VERSION}_${SCALA_VERSION}-${NESSIE_VERSION}.jar"
  "/path/to/nessie-client-${NESSIE_VERSION}.jar"
  "/path/to/nessie-iceberg-${NESSIE_VERSION}.jar"
)

# JAR 파일들을 콤마로 구분된 문자열로 변환
JARS_CLASSPATH=$(IFS=,; echo "${JARS[*]}")

# AWS 환경 변수 설정
# export AWS_ACCESS_KEY_ID="fw2DMsjZ6Hg3GsO7dygP"
# export AWS_SECRET_ACCESS_KEY="FnRONUp3jtb2vUbebqfP1ji95DIK7Ejm3zdxJGQU"
export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="minioadmin"
export AWS_REGION="ap-northeast-2"

# ThriftServer 실행
$SPARK_HOME/sbin/start-thriftserver.sh \
  --master local[*] \
  --driver-memory 4g \
  --executor-memory 4g \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.userlogs_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.userlogs_catalog.catalog-impl=org.apache.iceberg.hive.HiveCatalog \
  --conf spark.sql.catalog.userlogs_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.userlogs_catalog.warehouse=s3a://userlog-data/warehouse \
  --conf spark.sql.catalog.userlogs_catalog.s3.endpoint=http://54.180.166.228:9000 \
  --conf spark.sql.catalog.userlogs_catalog.s3.path-style-access=true \
  --hiveconf hive.metastore.uris=thrift://localhost:9083 \
  --hiveconf hive.metastore.warehouse.dir=s3a://userlog-data/warehouse \
  --hiveconf fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.sql.defaultCatalog=userlogs_catalog \
  --conf spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID} \
  --conf spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY} \
  --conf spark.hadoop.fs.s3a.endpoint=http://54.180.166.228:9000 \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.hive.metastore.client.connect.retry.delay=1s \
  --conf spark.hadoop.hive.metastore.client.socket.timeout=1800s \
  --jars $JARS_CLASSPATH \
  --hiveconf hive.server2.thrift.port=10000 
  # --hiveconf hive.server2.authentication=NOSASL 
  # --hiveconf hive.server2.transport.mode=binary \

  

