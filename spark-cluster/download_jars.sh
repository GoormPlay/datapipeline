#!/bin/bash

# 버전 정보 설정
SPARK_VERSION="3.5.5"
SCALA_VERSION="2.12"
ICEBERG_VERSION="1.5.0"
AWS_SDK_VERSION="2.20.160"
NESSIE_VERSION="0.76.1"

# 다운로드 디렉토리 생성
mkdir -p jars
cd jars

# Iceberg Spark Runtime JAR 다운로드
echo "Downloading Iceberg Spark Runtime JAR..."
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar

# Iceberg AWS Bundle JAR 다운로드
echo "Downloading Iceberg AWS Bundle JAR..."
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

# Nessie 관련 JAR 파일 다운로드
echo "Downloading Nessie JARs..."
wget https://repo1.maven.org/maven2/org/projectnessie/nessie-spark-extensions-3.5_2.12/${NESSIE_VERSION}/nessie-spark-extensions-3.5_2.12-${NESSIE_VERSION}.jar
wget https://repo1.maven.org/maven2/org/projectnessie/nessie-client/${NESSIE_VERSION}/nessie-client-${NESSIE_VERSION}.jar
wget https://repo1.maven.org/maven2/org/projectnessie/nessie-iceberg/${NESSIE_VERSION}/nessie-iceberg-${NESSIE_VERSION}.jar

echo "Downloaded all required JAR files to $(pwd) directory"
ls -la