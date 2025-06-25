import os
from typing import Dict, List, Optional
from fastapi import FastAPI, HTTPException, Query
from dotenv import load_dotenv
import pandas as pd
import traceback
import time
from datetime import datetime, timedelta
# PySpark 함수 및 타입 임포트
from pyspark.sql.functions import col, sum as _sum

# macOS에서의 fork 안전성 경고 해결
os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'

# 환경 변수 로드
load_dotenv()

app = FastAPI(
    title="Iceberg Analytics API",
    description="REST API for querying Iceberg tables, including analytics summaries."
)

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
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("S3_ACCESS_KEY", "LlZ4WcgdiQUlgizykTR0")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("S3_SECRET_KEY", "oUw1wYr2hvX3J5jupm5ytWc93HfOydGseLZejX9U")) \
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
                "message": "데이터베이스 조회 실패, 기본값으로 'analytics\' 사용",
                "catalog": catalog_name,
                "databases": ["analytics"]
            }
    except Exception as e:
        return {"error": str(e), "traceback": traceback.format_exc()}

# --- Analytics Endpoints ---

def refresh_and_get_data(table_name: str, group_by_cols: List[str], agg_col: str, agg_alias: str, limit: int):
    """Helper function to refresh table, query, and return data."""
    try:
        spark = get_spark()
        catalog_name = os.getenv('ICEBERG_CATALOG_NAME', 'userlogs_catalog')
        db_name = "analytics"
        full_table_name = f"{catalog_name}.{db_name}.`{table_name}`"

        # 테이블 메타데이터 새로고침
        try:
            spark.catalog.refreshTable(full_table_name)
            print(f"Refreshed table: {full_table_name}")
        except Exception as refresh_error:
            # 테이블이 아직 생성되지 않았을 수 있으므로 경고만 로깅
            print(f"Warning: Could not refresh table {full_table_name}. It might not exist yet. Error: {refresh_error}")

        df = spark.read.table(full_table_name)
        
        result_df = df.groupBy(*group_by_cols) \
                      .agg(_sum(agg_col).alias(agg_alias)) \
                      .orderBy(col(agg_alias).desc()) \
                      .limit(limit)
                      
        # FastAPI가 JSON으로 자동 변환하므로 to_dict() 사용
        return result_df.toPandas().to_dict(orient='records')

    except Exception as e:
        traceback.print_exc()
        # PySpark 오류 메시지가 더 유용할 수 있으므로 포함
        if "AnalysisException" in str(e):
            raise HTTPException(status_code=404, detail=f"Table or database not found. Check if '{db_name}.{table_name}' exists. Spark error: {e}")
        raise HTTPException(status_code=500, detail=f"데이터 조회 실패: {str(e)}")


@app.get("/analytics/user-interest", summary="사용자별 콘텐츠 관심도 TOP N", tags=["Analytics"])
def get_user_interest(limit: int = Query(10, ge=1, le=100)):
    """
    사용자별 콘텐츠 관심도 점수(좋아요, 리뷰, 평점 등)가 가장 높은 순으로 데이터를 반환합니다.
    """
    return refresh_and_get_data(
        table_name="user_content_interest_summary",
        group_by_cols=["userId", "videoId", "title"],
        agg_col="total_interest_score",
        agg_alias="total_score",
        limit=limit
    )

@app.get("/analytics/top-played", summary="가장 많이 재생된 콘텐츠 TOP N", tags=["Analytics"])
def get_top_played_content(limit: int = Query(10, ge=1, le=100)):
    """
    가장 많이 재생 시작된 콘텐츠 순으로 데이터를 반환합니다.
    """
    return refresh_and_get_data(
        table_name="content_play_summary",
        group_by_cols=["videoId", "title"],
        agg_col="play_start_count",
        agg_alias="total_plays",
        limit=limit
    )

@app.get("/analytics/top-recommended-clicks", summary="추천 클릭이 많은 콘텐츠 TOP N", tags=["Analytics"])
def get_top_recommendation_clicks(limit: int = Query(10, ge=1, le=100)):
    """
    추천을 통해 가장 많이 클릭된 콘텐츠 순으로 데이터를 반환합니다.
    """
    return refresh_and_get_data(
        table_name="recommendation_click_summary",
        group_by_cols=["videoId", "title"],
        agg_col="recom_click_count",
        agg_alias="total_recom_clicks",
        limit=limit
    )

@app.get("/analytics/top-clicks", summary="단순 클릭이 많은 콘텐츠 TOP N (기존)", tags=["Analytics"])
def get_top_clicks(limit: int = Query(10, ge=1, le=100)):
    """(기존) 가장 많이 클릭된 콘텐츠(content_click) 순으로 데이터를 반환합니다."""
    return refresh_and_get_data(
        table_name="video_clicks_summary",
        group_by_cols=["videoId", "title"],
        agg_col="click_count",
        agg_alias="total_clicks",
        limit=limit
    )

@app.get("/analytics/recent-user-interest", summary="최근 N분간의 사용자 관심도", tags=["Analytics"])
def get_recent_user_interest(
    minutes_ago: int = Query(30, ge=1, le=1440, description="조회할 최근 시간(분)"),
    limit: int = Query(10, ge=1, le=100)
):
    """
    지정된 시간(기본 30분) 내에 발생한 사용자 관심도 데이터를 집계하여 반환합니다.
    """
    try:
        spark = get_spark()
        table_name = f"{os.getenv('ICEBERG_CATALOG_NAME', 'userlogs_catalog')}.analytics.user_content_interest_summary"
        
        # 시간 필터링 조건 생성
        time_threshold = datetime.utcnow() - timedelta(minutes=minutes_ago)
        time_threshold_str = time_threshold.strftime('%Y-%m-%d %H:%M:%S')

        df = spark.read.table(table_name)
        
        result_df = df.filter(col("window_start") >= time_threshold_str) \
                      .groupBy("userId", "videoId", "title") \
                      .agg(_sum("total_interest_score").alias("recent_score")) \
                      .orderBy(col("recent_score").desc()).limit(limit)
        return result_df.toPandas().to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"최근 관심도 데이터 조회 실패: {str(e)}")

@app.get("/tables")
def list_tables(database: str = "analytics"):
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
    
@app.get("/table-data")
def get_table_data(
    table: str,
    database: str = "analytics",
    limit: int = 100,
    where_clause: Optional[str] = None,
    columns: Optional[str] = None
):
    """테이블 데이터 조회"""
    try:
        spark = get_spark()
        catalog_name = os.getenv('ICEBERG_CATALOG_NAME', 'userlogs_catalog')
        
        # 테이블 이름에 특수문자(-) 포함 시 백틱으로 감싸기
        safe_table_name = f"`{table}`" if '-' in table else table
        safe_database_name = f"`{database}`" if '-' in database else database
        
        # 기본 쿼리 구성
        select_clause = "*" if not columns else columns
        base_query = f"SELECT {select_clause} FROM {catalog_name}.{safe_database_name}.{safe_table_name}"
        
        # WHERE 절 추가 (제공된 경우)
        if where_clause:
            query = f"{base_query} WHERE {where_clause}"
        else:
            query = base_query
            
        # LIMIT 절 추가
        full_query = f"{query} LIMIT {limit}"
        
        try:
            print(f"실행할 쿼리: {full_query}")
            result_df = spark.sql(full_query)
            
            # DataFrame을 Python 딕셔너리 목록으로 변환
            rows = [row.asDict() for row in result_df.collect()]
            
            # 스키마 정보도 추가
            schema = [{"name": field.name, "type": str(field.dataType)} 
                     for field in result_df.schema.fields]
            
            return {
                "catalog": catalog_name,
                "database": database,
                "table": table,
                "schema": schema,
                "rowCount": len(rows),
                "data": rows
            }
        except Exception as e1:
            return {
                "error": str(e1),
                "message": f"{catalog_name}.{database}.{table}의 데이터 조회 실패",
                "query": full_query
            }
    except Exception as e:
        return {"error": str(e), "traceback": traceback.format_exc()}
    
@app.get("/table-schema")
def get_table_schema(table: str, database: str = 'analytics'):
    """테이블 스키마 조회"""
    try:
        spark = get_spark()
        catalog_name = os.getenv('ICEBERG_CATALOG_NAME', 'userlogs_catalog')
        
        try:
            schema_df = spark.sql(f"DESCRIBE TABLE {catalog_name}.{database}.{table}")
            
            # 스키마 정보를 Python 리스트로 변환
            schema = [row.asDict() for row in schema_df.collect()]
            
            return {
                "catalog": catalog_name, 
                "database": database, 
                "table": table, 
                "schema": schema
            }
        except Exception as e1:
            return {
                "error": str(e1),
                "message": f"{catalog_name}.{database}.{table}의 스키마 조회 실패"
            }
    except Exception as e:
        return {"error": str(e), "traceback": traceback.format_exc()}

@app.get("/table-metadata")
def get_table_metadata(table: str, database: str = 'analytics'):
    """테이블 메타데이터 조회 (히스토리, 스냅샷 등)"""
    try:
        spark = get_spark()
        catalog_name = os.getenv('ICEBERG_CATALOG_NAME', 'userlogs_catalog')
        
        result = {
            "catalog": catalog_name,
            "database": database,
            "table": table,
        }
        
        try:
            # 히스토리 정보
            history_df = spark.sql(f"SELECT * FROM {catalog_name}.{database}.{table}.history")
            result["history"] = [row.asDict() for row in history_df.collect()]
            
            # 스냅샷 정보
            snapshots_df = spark.sql(f"SELECT * FROM {catalog_name}.{database}.{table}.snapshots")
            result["snapshots"] = [row.asDict() for row in snapshots_df.collect()]
            
            # 파일 정보 (선택 사항 - 데이터가 많을 수 있음)
            # files_df = spark.sql(f"SELECT * FROM {catalog_name}.{database}.{table}.files")
            # result["files"] = [row.asDict() for row in files_df.collect()]
            
            return result
        except Exception as e1:
            return {
                "error": str(e1),
                "message": f"{catalog_name}.{database}.{table}의 메타데이터 조회 실패"
            }
    except Exception as e:
        return {"error": str(e), "traceback": traceback.format_exc()}

@app.get("/table-stats")
def get_table_stats(table: str, database: str = 'analytics'):
    """테이블 통계 정보 조회"""
    try:
        spark = get_spark()
        catalog_name = os.getenv('ICEBERG_CATALOG_NAME', 'userlogs_catalog')
        
        try:
            # 행 수 카운트
            count_df = spark.sql(f"SELECT COUNT(*) as row_count FROM {catalog_name}.{database}.`{table}`")
            row_count = count_df.collect()[0]["row_count"]
            
            # 테이블 파일 통계
            file_stats_df = spark.sql(f"""
                SELECT 
                    COUNT(*) as file_count,
                    SUM(file_size_in_bytes) as total_size_bytes,
                    AVG(file_size_in_bytes) as avg_file_size_bytes
                FROM {catalog_name}.{database}.`{table}`.files
            """)
            file_stats = file_stats_df.collect()[0].asDict() if file_stats_df.count() > 0 else {}
            
            return {
                "catalog": catalog_name,
                "database": database,
                "table": table,
                "row_count": row_count,
                "file_stats": file_stats
            }
        except Exception as e1:
            return {
                "error": str(e1),
                "message": f"{catalog_name}.{database}.`{table}`의 통계 정보 조회 실패"
            }
    except Exception as e:
        return {"error": str(e), "traceback": traceback.format_exc()}

@app.get("/run-query")
def run_custom_query(query: str, limit: int = 1000):
    """사용자 정의 쿼리 실행"""
    try:
        spark = get_spark()
        
        # 안전을 위해 LIMIT 적용
        if "LIMIT" not in query.upper():
            query = f"{query} LIMIT {limit}"
        
        try:
            start_time = time.time()
            result_df = spark.sql(query)
            end_time = time.time()
            
            # 결과를 Python 딕셔너리로 변환
            rows = [row.asDict() for row in result_df.collect()]
            
            return {
                "query": query,
                "execution_time_ms": int((end_time - start_time) * 1000),
                "row_count": len(rows),
                "data": rows
            }
        except Exception as e1:
            return {
                "error": str(e1),
                "message": "쿼리 실행 실패",
                "query": query
            }
    except Exception as e:
        return {"error": str(e), "traceback": traceback.format_exc()}
    


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("streaming_iceberg_api:app", host="0.0.0.0", port=8000, reload=True)