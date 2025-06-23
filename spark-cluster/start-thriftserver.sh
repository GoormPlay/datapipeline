#!/bin/bash

# Spark 설정 (config.yaml 참조)
SPARK_MASTER="local" # config.yaml: spark.master
# 필요한 패키지
SPARK_PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262"
# ,org.apache.httpcomponents:httpclient:4.5.13,org.apache.httpcomponents:httpcore:4.4.15,org.apache.iceberg:iceberg-aws:1.3.1
# Iceberg 설정 (config.yaml: iceberg 참조)
ICEBERG_CATALOG_NAME="userlogs_catalog" # config.yaml: iceberg.catalog_name
ICEBERG_WAREHOUSE_PATH="s3a://userlog-data/warehouse" # config.yaml: iceberg.warehouse_path
ICEBERG_DB_NAME="analytics" # config.yaml: iceberg.db_name (SQLAlchemy URI에 사용될 수 있음)

# S3 설정 (config.yaml: s3 참조)
S3_ENDPOINT="http://54.180.166.228:9000" # config.yaml: s3.endpoint
S3_ACCESS_KEY="qpJ3MNpmCtpMQw26BURO"
S3_SECRET_KEY="xhrkNGpcVVozn8sAgI7xPsoTfqUxZJgOTwko4DRd"
S3_REGION="ap-northeast-2" # config.yaml: s3.region
S3_PATH_STYLE_ACCESS="true" # config.yaml: s3.path_style_access
S3_CONNECTION_SSL_ENABLED="false" # config.yaml: s3.connection_ssl_enabled

# Thrift Server 포트 (기본값: 10000)
THRIFT_PORT="10000"

echo "Starting Spark Thrift Server..."
$SPARK_HOME/sbin/start-thriftserver.sh \
  --driver-memory 4g \
  --conf spark.executor.memory=4g \
  --hiveconf hive.server2.thrift.port=10000 \
  --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
  --name "Thrift JDBC/ODBC Server" \
  --master local \

            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config(f"spark.sql.catalog.{os.getenv('ICEBERG_CATALOG_NAME', 'userlogs_catalog')}", "org.apache.iceberg.spark.SparkCatalog") \
            .config(f"spark.sql.catalog.{os.getenv('ICEBERG_CATALOG_NAME', 'userlogs_catalog')}.type", "hadoop") \
            .config(f"spark.sql.catalog.{os.getenv('ICEBERG_CATALOG_NAME', 'userlogs_catalog')}.warehouse", os.getenv("ICEBERG_WAREHOUSE", "s3a://userlog-data/warehouse")) \
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://54.180.166.228:9000")) \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("S3_ACCESS_KEY", "qpJ3MNpmCtpMQw26BURO")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("S3_SECRET_KEY", "xhrkNGpcVVozn8sAgI7xPsoTfqUxZJgOTwko4DRd")) \
            .config("spark.haddop.fs.s3a.region", os.getenv("S3_REGION", "ap-northeast-2")) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# spark = SparkSession.builder \
#         .appName("KafkaToIcebergETL") \
#         .config("spark.driver.bindAddress", "127.0.0.1") \
#         .config("spark.driver.host", "127.0.0.1") \
#         .config(f"spark.sql.catalog.{args.iceberg_catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
#         .config(f"spark.sql.catalog.{args.iceberg_catalog_name}.type", "hadoop") \
#         .config(f"spark.sql.catalog.{args.iceberg_catalog_name}.warehouse", args.iceberg_warehouse_path) \
#         .config("spark.hadoop.fs.s3a.endpoint", args.s3_endpoint) \
#         .config("spark.hadoop.fs.s3a.access.key", args.s3_access_key) \
#         .config("spark.hadoop.fs.s3a.secret.key", args.s3_secret_key) \
#         .config("spark.hadoop.fs.s3a.region", args.s3_region) \
#         .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
#         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
#         .getOrCreate()
  # --hiveconf hive.server2.authentication=NOSASL \
  # --hiveconf hive.server2.enable.doAs=false \
  # --hiveconf hive.server2.transport.mode=binary \
  # --hiveconf hive.server2.thrift.max.worker.threads=100 \
  # --hiveconf hive.server2.thrift.min.worker.threads=50 \
  # --hiveconf hive.server2.async.exec.threads=50 \
  # --hiveconf hive.server2.async.exec.wait.queue.size=100 \

  # # --hiveconf hive.server2.thrift.protocol=BINARY \
  # # --hiveconf hive.server2.thrift.client.protocol.version=6 \
  # # --hiveconf hive.server2.thrift.min.protocol.version=5 \
  # # --hiveconf hive.server2.thrift.max.protocol.version=9 \
  # # --hiveconf hive.server2.allow.user.substitution=true \
