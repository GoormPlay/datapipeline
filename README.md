# 📡 Kafka → Spark Structured Streaming → Iceberg 스트리밍 ETL 파이프라인

이 프로젝트는 Kafka로 수집된 실시간 사용자 로그 데이터를 Spark Structured Streaming을 통해 처리하고, Apache Iceberg 테이블에 적재하는 **실시간 ETL 파이프라인**입니다.
추가적으로, API 서버와 Streamlit 대시보드를 통해 집계 결과 및 원장 데이터를 실시간으로 조회할 수 있도록 구성했습니다.

---

## 🔧 기술 스택

* **Kafka**: 실시간 로그 수집 및 스트리밍 전송
* **Schema Registry (Avro)**: 스키마 관리 및 디코딩
* **Spark Structured Streaming**: 실시간 데이터 처리 및 집계
* **Apache Iceberg**: 테이블 포맷 (MERGE INTO 기반 Upsert 지원)
* **MinIO (S3 compatible)**: Iceberg 저장소 및 메타데이터 관리
* **Slack Webhook**: 스키마 변경/ETL 실패 등 알림 전송
* **FastAPI**: Iceberg 데이터 API 서버 구현
* **Streamlit**: 실시간 분석용 대시보드 시각화

---

## 📌 주요 기능

### 실시간 데이터 수집 및 ETL

* Kafka 토픽으로부터 사용자 로그 수신
* Spark에서 Avro 디코딩 및 Spark SQL 기반 ETL 처리
* Iceberg 테이블에 다음과 같이 저장:

  * Raw 로그 테이블 (Bronze)
  * 집계 테이블 (Silver/Gold): `MERGE INTO` 기반 Upsert
* 스키마 변경 감지 시

  * S3에 새로운 스키마 저장
  * Slack Webhook으로 알림 전송

### API 서버 (FastAPI 기반)

* SparkSession을 활용해 Iceberg 테이블 조회
* 주요 엔드포인트:

  * `/analytics/user-interest`: 사용자별 관심도 높은 콘텐츠 조회
  * `/analytics/top-played`: 가장 많이 재생된 콘텐츠 조회
  * `/analytics/recent-*`: 최근 30분 기준 집계 정보 조회
  * `/table-data`: 특정 테이블의 원본 로그 데이터 조회
  * `/run-query`: 커스텀 Spark SQL 실행

### 대시보드 시각화 (Streamlit 기반)

* 실시간 분석 탭: 최근 30분간 사용자 행동 데이터 시각화
* 관심도 / 재생 / 추천 클릭 / 전체 클릭 집계
* 종합 분석 탭: 누적 기준 상위 콘텐츠 순위 확인
* 원본 로그 조회 탭: `user_logs` 테이블 직접 필터링 및 확인


---

## 🧱 Iceberg 테이블 구조

| 테이블 이름                          | 설명                       |
| ------------------------------- | --------------------------- |
| `bronze_data`                   | Kafka 원장 데이터 저장          |
| `user_logs`                     | 사용자 행동 로그 원장 데이터 저장   |
| `video_clicks_summary`          | 콘텐츠 클릭 집계 (30분 단위)      |
| `user_content_interest_summary` | 사용자 관심도 점수 집계           |
| `content_play_summary`          | 콘텐츠 재생 시작 횟수 집계         |
| `recommendation_click_summary`  | 추천 콘텐츠 클릭 집계             |

---

## 🚀 실행 방법

### 1. Spark Streaming Job 실행

```bash
python run_spark_job.py --job streaming_iceberg_table.py
```

### 2. FastAPI 서버 실행

```bash
uvicorn streaming_iceberg_api:app --host 0.0.0.0 --port 8000 --reload
```

### 3. Streamlit 대시보드 실행

```bash
streamlit run dashboard/app.py
```

---

## 📝 참고사항

* Iceberg 테이블은 Spark 3.5 기반에서 동작하도록 설정되었습니다.
* MinIO는 S3 compatible 방식으로 운영되며, `.env`를 통해 접속 설정을 관리합니다.
* Slack 알림은 Schema 변경 감지, ETL 실패 시 트리거됩니다.
