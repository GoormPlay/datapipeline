import os
from typing import Dict, List, Optional
from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv
import pandas as pd
import traceback
import time

# macOS에서의 fork 안전성 경고 해결
os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'

# 환경 변수 로드
load_dotenv()

app = FastAPI(title="Simple Iceberg Table API", description="REST API for querying Iceberg tables")

# 글로벌 SparkSession
spark = None

def get_spark():
    global spark
    
    if spark is not None:
        return spark
        
    try:
        print("SparkSession 생성 시작...")
        start_time = time.time()
        
        # PySpark 임포트는 환경 변수 설정 후에 해야 함
        from pyspark.sql import SparkSession
        
        # 중요: 버전 호환성 문제 - SparkSession 생성 방식 변경
        builder = SparkSession.builder \
            .appName("SimpleIcebergAPI") \
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
            
        # 이 코드가 중요: master 설정을 별도로 수행
        builder = builder.master("local[*]")
        
        # getOrCreate 메서드 호출
        spark = builder.getOrCreate()
            
        # 버전 출력
        print(f"SparkSession 생성 완료 - Spark 버전: {spark.version}")
        
        end_time = time.time()
        print(f"SparkSession 생성 시간: {end_time - start_time:.2f}초")
            
        return spark
            
    except Exception as e:
        print(f"SparkSession 생성 중 오류 발생: {e}")
        traceback.print_exc()
        raise

@app.get("/")
def read_root():
    return {"message": "Simple Iceberg API is running"}

@app.get("/spark-version")
def get_spark_version():
    """Spark 버전 확인"""
    try:
        spark = get_spark()
        return {"spark_version": spark.version}
    except Exception as e:
        return {"error": str(e)}

@app.get("/databases")
def list_databases():
    """데이터베이스 목록 조회"""
    try:
        spark = get_spark()
        catalog_name = os.getenv('ICEBERG_CATALOG_NAME', 'userlogs_catalog')
        
        try:
            # sql 메서드에 단일 문자열 인수만 전달
            databases_df = spark.sql(f"SHOW DATABASES IN {catalog_name}")
            databases = [row["databaseName"] for row in databases_df.collect()]
            return {"catalog": catalog_name, "databases": databases}
        except Exception as e1:
            return {
                "error": str(e1),
                "message": "데이터베이스 조회 실패, 기본값으로 'default\' 사용",
                "catalog": catalog_name,
                "databases": ["default"]
            }
    except Exception as e:
        return {"error": str(e), "traceback": traceback.format_exc()}

@app.get("/tables")
def list_tables(database: str = "default"):
    """테이블 목록 조회"""
    try:
        spark = get_spark()
        catalog_name = os.getenv('ICEBERG_CATALOG_NAME', 'userlogs_catalog')
        
        try:
            # 단순한 SQL 쿼리 실행
            tables_df = spark.sql(f"SHOW TABLES IN {catalog_name}.{database}")
            tables = [row["tableName"] for row in tables_df.collect()]
            return {"catalog": catalog_name, "database": database, "tables": tables}
        except Exception as e1:
            return {
                "error": str(e1),
                "message": f"{catalog_name}.{database}의 테이블 목록 조회 실패",
                "catalog": catalog_name, 
                "database": database,
                "tables": []
            }
    except Exception as e:
        return {"error": str(e), "traceback": traceback.format_exc()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("simple_iceberg_api:app", host="0.0.0.0", port=8000, reload=True)