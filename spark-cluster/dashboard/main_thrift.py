from fastapi import FastAPI, HTTPException, Depends, Query
from pydantic import BaseModel, Field
from typing import List
from datetime import datetime, date
import os

# Spark Thrift Server에 연결하기 위한 라이브러리
try:
    from pyhive import hive
except ImportError:
    # PyHive가 설치되지 않았을 경우를 대비한 가드
    hive = None

# --- 1. API 응답 형식을 정의하는 Pydantic 모델 ---
class VideoClickSummary(BaseModel):
    window_start: datetime
    window_end: datetime
    video_id: str = Field(..., alias="videoId")
    title: str
    click_count: int

    class Config:
        # alias 필드(videoId)를 사용하여 Python 객체를 생성할 수 있도록 허용
        allow_population_by_field_name = True

# --- 2. Spark Thrift Server 연결을 관리하는 부분 ---
# 실제 운영 환경에서는 환경 변수나 Secret Manager를 통해 접속 정보를 관리합니다.
THRIFT_HOST = os.getenv("THRIFT_HOST", "localhost")  # Thrift Server가 실행 중인 호스트
THRIFT_PORT = int(os.getenv("THRIFT_PORT", 10000)) # Thrift Server의 기본 포트
THRIFT_USER = os.getenv("THRIFT_USER", "fastapi_user")
ICEBERG_DB_NAME = "analytics" # Spark Job에서 사용한 데이터베이스 이름

def get_thrift_cursor():
    """Spark Thrift Server 커넥션을 생성하고 커서를 반환하는 Dependency"""
    if not hive:
        raise ImportError("PyHive client library not found. Run 'pip install \"PyHive[thrift]\"'")
    
    conn = None
    try:
        # Thrift Server에 연결
        conn = hive.connect(host=THRIFT_HOST, port=THRIFT_PORT, username=THRIFT_USER)
        cursor = conn.cursor()
        # 쿼리를 실행할 데이터베이스를 명시적으로 선택
        cursor.execute(f"USE {ICEBERG_DB_NAME}")
        yield cursor
    except Exception as e:
        # 운영 환경에서는 에러를 로깅하는 것이 중요합니다.
        print(f"Thrift Server connection failed: {e}")
        raise HTTPException(status_code=503, detail="Database connection is unavailable.")
    finally:
        if conn:
            conn.close()

# --- 3. FastAPI 애플리케이션 정의 ---
app = FastAPI(
    title="OTT 서비스 분석 API (with Spark Thrift)",
    description="Spark Thrift Server를 통해 Iceberg 데이터를 조회하는 API입니다.",
    version="1.0.0",
)

# --- 4. API 엔드포인트 정의 ---
@app.get(
    "/summary/top-video-clicks",
    response_model=List[VideoClickSummary],
    summary="기간 내 인기 비디오 순위 조회",
    tags=["Summary"],
)
async def get_top_video_clicks(
    target_date: date,
    limit: int = Query(10, ge=1, le=100, description="조회할 순위 개수"),
    cursor = Depends(get_thrift_cursor) # Dependency Injection으로 커서 받기
):
    """
    특정 날짜의 비디오 클릭 수를 기준으로 가장 인기 있는 비디오 순위를 반환합니다.
    """
    try:
        # Spark Thrift Server를 통해 Iceberg 테이블을 조회하는 표준 SQL
        table_name = "video_clicks_summary"
        query = f"""
        SELECT
            window_start,
            window_end,
            videoId,
            title,
            click_count
        FROM {table_name}
        WHERE CAST(window_start AS DATE) = DATE '{target_date}'
        ORDER BY click_count DESC
        LIMIT {limit}
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # PyHive는 컬럼명을 '테이블명.컬럼명' 형식으로 반환할 수 있으므로, 순수 컬럼명만 추출
        columns = [desc[0].split('.')[-1] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in rows]
        
        return results

    except Exception as e:
        print(f"Query execution failed: {e}")
        raise HTTPException(status_code=500, detail="데이터 조회 중 오류가 발생했습니다.")

# 서버 실행 방법 (터미널에서):
# uvicorn main_thrift:app --reload
