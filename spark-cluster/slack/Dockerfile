FROM python:3.9-slim

# Java 설치 (PySpark 요구사항)
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 필요한 Python 패키지 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 코드 복사
COPY iceberg_spark_api.py .
COPY .env .

# 포트 노출
EXPOSE 8000

# 애플리케이션 실행
CMD ["uvicorn", "spark_iceberg_api:app", "--host", "0.0.0.0", "--port", "8000"]