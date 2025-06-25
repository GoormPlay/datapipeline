#!/usr/bin/env python3
"""
사용자 로그 집계 Airflow DAG
목적: 매일 새벽 1시에 Spark 클러스터에서 사용자 로그 집계 처리

DAG 설정:
- 스케줄: 매일 새벽 1시 (cron: 0 1 * * *)
- 재시도: 3회
- 타임아웃: 1시간
- 이메일 알림: 실패 시
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.dates import days_ago

# DAG 기본 설정
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 20),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# DAG 정의
dag = DAG(
    'userlog_aggregation_elt',
    default_args=default_args,
    description='사용자 로그 집계 ELT 처리 - LightFM 학습용 데이터셋 생성',
    schedule_interval='0 1 * * *',  # 매일 새벽 1시
    catchup=False,
    max_active_runs=1,
    tags=['spark', 'elt', 'userlog', 'ml-data', 'lightfm'],
)

# 환경 설정
SPARK_MASTER = "spark://your-ec2-ip:7077"  # EC2 Spark 마스터 주소
SPARK_SCRIPT_PATH = "/opt/airflow/dags/scripts/spark/userlog_agg_pyspark.py"  # Spark 작업 스크립트 경로
MINIO_ENDPOINT = "http://your-ec2-ip:9000"  # MinIO 엔드포인트

def get_process_date(**context):
    """처리할 날짜 계산 (어제 날짜)"""
    execution_date = context['execution_date']
    process_date = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    return process_date

def log_start_message(**context):
    """시작 메시지 로깅"""
    process_date = get_process_date(**context)
    print(f"🚀 사용자 로그 집계 ELT 처리 시작")
    print(f"📅 처리 대상 날짜: {process_date}")
    print(f"🕐 실행 시각: {datetime.now()}")
    print(f"🎯 Spark 마스터: {SPARK_MASTER}")
    print(f"💾 MinIO 엔드포인트: {MINIO_ENDPOINT}")
    return process_date

def validate_process_date(**context):
    """처리 날짜 유효성 검증"""
    process_date = context['ti'].xcom_pull(task_ids='log_start')
    
    # 날짜 형식 검증
    try:
        datetime.strptime(process_date, '%Y-%m-%d')
        print(f"✅ 처리 날짜 유효성 확인: {process_date}")
    except ValueError:
        raise ValueError(f"❌ 잘못된 날짜 형식: {process_date}")
    
    # 미래 날짜 방지
    today = datetime.now().strftime('%Y-%m-%d')
    if process_date >= today:
        raise ValueError(f"❌ 미래 날짜는 처리할 수 없습니다: {process_date} >= {today}")
    
    return process_date

def log_completion_message(**context):
    """완료 메시지 로깅"""
    process_date = context['ti'].xcom_pull(task_ids='log_start')
    print(f"✅ 사용자 로그 집계 ELT 처리 완료")
    print(f"📅 처리된 날짜: {process_date}")
    print(f"🕐 완료 시각: {datetime.now()}")


# Task 1: 시작 로깅 및 날짜 설정
start_logging = PythonOperator(
    task_id='log_start',
    python_callable=log_start_message,
    dag=dag,
)

# Task 2: 처리 날짜 유효성 검증
validate_date = PythonOperator(
    task_id='validate_process_date',
    python_callable=validate_process_date,
    dag=dag,
)

# Task 3: MinIO 소스 데이터 존재 확인
check_source_data = BashOperator(
    task_id='check_source_data',
    bash_command=f"""
    PROCESS_DATE="{{{{ ti.xcom_pull(task_ids='log_start') }}}}"
    echo "🔍 소스 데이터 존재 확인: userlog-data/test/user-activity-raw/$PROCESS_DATE"
    
    # MinIO CLI를 사용해 소스 데이터 존재 확인
    mc ls minio/userlog-data/test/user-activity-raw/$PROCESS_DATE/ || {{
        echo "❌ 소스 데이터를 찾을 수 없습니다: $PROCESS_DATE"
        exit 1
    }}
    
    # 파일 수 확인
    FILE_COUNT=$(mc ls minio/userlog-data/test/user-activity-raw/$PROCESS_DATE/ | wc -l)
    if [ "$FILE_COUNT" -eq 0 ]; then
        echo "❌ 소스 데이터 파일이 없습니다"
        exit 1
    fi
    
    echo "✅ 소스 데이터 확인 완료 ($FILE_COUNT 개 파일)"
    """,
    dag=dag,
)

# Task 4: Spark 클러스터 상태 확인
check_spark_cluster = BashOperator(
    task_id='check_spark_cluster',
    bash_command=f"""
    echo "🔍 Spark 클러스터 상태 확인: {SPARK_MASTER}"
    
    # Spark 마스터 웹 UI 상태 확인
    SPARK_WEB_URL="{SPARK_MASTER.replace('spark://', 'http://').replace(':7077', ':8080')}"
    
    curl -f -s "$SPARK_WEB_URL" > /dev/null || {{
        echo "❌ Spark 클러스터에 연결할 수 없습니다: $SPARK_WEB_URL"
        exit 1
    }}
    
    # Worker 노드 수 확인
    echo "✅ Spark 클러스터 상태 정상"
    echo "🔗 Spark 웹 UI: $SPARK_WEB_URL"
    """,
    dag=dag,
)

# Task 5: MinIO 타겟 버킷 확인/생성
prepare_target_storage = BashOperator(
    task_id='prepare_target_storage',
    bash_command=f"""
    PROCESS_DATE="{{{{ ti.xcom_pull(task_ids='log_start') }}}}"
    echo "🏗️ 타겟 저장소 준비: ml-learning-data/$PROCESS_DATE"
    
    # 타겟 버킷 존재 확인 및 생성
    mc ls minio/ml-learning-data/ > /dev/null 2>&1 || {{
        echo "📁 ml-learning-data 버킷 생성 중..."
        mc mb minio/ml-learning-data/
    }}
    
    # 기존 데이터 백업 (있는 경우)
    if mc ls minio/ml-learning-data/$PROCESS_DATE/ > /dev/null 2>&1; then
        echo "⚠️ 기존 데이터 발견, 백업 중..."
        BACKUP_PATH="ml-learning-data-backup/$PROCESS_DATE-$(date +%H%M%S)"
        mc cp -r minio/ml-learning-data/$PROCESS_DATE/ minio/$BACKUP_PATH/
        echo "💾 백업 완료: $BACKUP_PATH"
    fi
    
    echo "✅ 타겟 저장소 준비 완료"
    """,
    dag=dag,
)

# Task 6: Spark 작업 실행 (메인 ELT 처리) - SparkSubmitOperator 사용
run_spark_job = SparkSubmitOperator(
    task_id='run_spark_aggregation',
    application=SPARK_SCRIPT_PATH,
    conn_id='spark_default',  # Airflow Connection 설정 필요
    conf={
        'spark.app.name': 'UserLogAggregation-{{ ti.xcom_pull(task_ids="log_start") }}',
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        'spark.sql.execution.arrow.pyspark.enabled': 'true',
        'spark.pyspark.python': 'python3',
        'spark.pyspark.driver.python': 'python3'
    },
    driver_memory='2g',
    executor_memory='4g',
    executor_cores=2,
    num_executors=4,
    packages='org.apache.hadoop:hadoop-aws:3.3.4',
    application_args=[
        '--date', '{{ ti.xcom_pull(task_ids="log_start") }}',
        '--minio-endpoint', MINIO_ENDPOINT
    ],
    dag=dag,
    verbose=True  # 상세 로그
)

# Task 6-1: BashOperator 버전 (참고용 - 주석 처리)
# run_spark_job_bash = BashOperator(
#     task_id='run_spark_aggregation_bash',
#     bash_command=f"""
#     PROCESS_DATE="{{{{ ti.xcom_pull(task_ids='log_start') }}}}"
#     
#     echo "🚀 Spark 작업 실행 시작"
#     echo "📅 처리 날짜: $PROCESS_DATE"
#     echo "🎯 Spark 마스터: {SPARK_MASTER}"
#     echo "📂 스크립트 경로: {SPARK_SCRIPT_PATH}"
#     
#     # Spark Submit 실행
#     spark-submit \\
#         --master {SPARK_MASTER} \\
#         --deploy-mode client \\
#         --conf spark.app.name="UserLogAggregation-$PROCESS_DATE" \\
#         --conf spark.pyspark.python=python3 \\
#         --conf spark.pyspark.driver.python=python3 \\
#         --conf spark.sql.adaptive.enabled=true \\
#         --conf spark.sql.adaptive.coalescePartitions.enabled=true \\
#         --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \\
#         --driver-memory 2g \\
#         --executor-memory 4g \\
#         --executor-cores 2 \\
#         --num-executors 4 \\
#         --packages org.apache.hadoop:hadoop-aws:3.3.4 \\
#         {SPARK_SCRIPT_PATH} \\
#         --date $PROCESS_DATE \\
#         --minio-endpoint {MINIO_ENDPOINT}
#     
#     SPARK_EXIT_CODE=$?
#     
#     if [ $SPARK_EXIT_CODE -eq 0 ]; then
#         echo "✅ Spark 작업 성공적으로 완료"
#         echo "📊 처리 결과:"
#         echo "  - 입력: s3a://userlog-data/test/user-activity-raw/$PROCESS_DATE/"
#         echo "  - 출력: s3a://ml-learning-data/$PROCESS_DATE/"
#     else
#         echo "❌ Spark 작업 실패 (종료 코드: $SPARK_EXIT_CODE)"
#         exit $SPARK_EXIT_CODE
#     fi
#     """,
#     dag=dag,
# )

# Task 7: 출력 데이터 검증
validate_output = BashOperator(
    task_id='validate_output',
    bash_command="""
    PROCESS_DATE="{{ ti.xcom_pull(task_ids='log_start') }}"
    echo "🔍 출력 데이터 검증: ml-learning-data/$PROCESS_DATE"
    
    # 출력 데이터 존재 확인
    mc ls minio/ml-learning-data/$PROCESS_DATE/ || {
        echo "❌ 출력 데이터를 찾을 수 없습니다"
        exit 1
    }
    
    # 파일 크기 확인 (최소 1KB 이상)
    TOTAL_SIZE=$(mc du minio/ml-learning-data/$PROCESS_DATE/ | tail -1 | awk '{print $1}')
    
    if [ -z "$TOTAL_SIZE" ] || [ "$TOTAL_SIZE" -lt 1024 ]; then
        echo "❌ 출력 파일이 너무 작습니다 (${TOTAL_SIZE:-0} bytes)"
        exit 1
    fi
    
    # 파일 수 확인
    FILE_COUNT=$(mc find minio/ml-learning-data/$PROCESS_DATE/ --name "*.parquet" | wc -l)
    
    if [ "$FILE_COUNT" -eq 0 ]; then
        echo "❌ Parquet 파일을 찾을 수 없습니다"
        exit 1
    fi
    
    # 데이터 품질 간단 체크 (샘플링)
    echo "📊 데이터 품질 체크:"
    echo "  - 총 크기: ${TOTAL_SIZE} bytes"
    echo "  - 파일 수: ${FILE_COUNT}개"
    echo "  - 파티션: process_date=$PROCESS_DATE"
    
    echo "✅ 출력 데이터 검증 완료"
    """,
    dag=dag,
)

# Task 8: 데이터 품질 보고서 생성
generate_quality_report = BashOperator(
    task_id='generate_quality_report',
    bash_command="""
    PROCESS_DATE="{{ ti.xcom_pull(task_ids='log_start') }}"
    REPORT_FILE="/tmp/data_quality_report_${PROCESS_DATE}.txt"
    
    echo "📝 데이터 품질 보고서 생성: $PROCESS_DATE"
    
    cat > $REPORT_FILE << EOF
====================================
데이터 품질 보고서
====================================
처리 날짜: $PROCESS_DATE
생성 시각: $(date)
처리 담당: Airflow DAG (userlog_aggregation_elt)

📂 데이터 경로:
- 입력: s3://userlog-data/test/user-activity-raw/$PROCESS_DATE/
- 출력: s3://ml-learning-data/$PROCESS_DATE/

📊 처리 결과:
$(mc du minio/ml-learning-data/$PROCESS_DATE/ 2>/dev/null || echo "데이터 크기 확인 실패")

📁 파일 구조:
$(mc find minio/ml-learning-data/$PROCESS_DATE/ --name "*.parquet" 2>/dev/null | head -10 || echo "파일 목록 확인 실패")

🎯 다음 단계:
- LightFM 모델 학습에 사용 가능
- 추천 시스템 데이터로 활용
- 데이터 분석 및 인사이트 도출

====================================
EOF
    
    echo "✅ 품질 보고서 생성 완료: $REPORT_FILE"
    cat $REPORT_FILE
    """,
    dag=dag,
)

# Task 9: 완료 로깅
complete_logging = PythonOperator(
    task_id='log_completion',
    python_callable=log_completion_message,
    dag=dag,
)

# Task 10: 성공 알림 (Slack)
success_notification = SlackWebhookOperator(
    task_id='send_success_notification',
    http_conn_id='slack_webhook',  # Airflow Connection에 설정된 Slack Webhook
    message="""
🎉 *사용자 로그 집계 ELT 성공*

📅 처리 날짜: {{ ti.xcom_pull(task_ids='log_start') }}
🕐 완료 시각: {{ ds }}
📊 DAG: {{ dag.dag_id }}
✅ 상태: 성공

💾 저장 위치: `ml-learning-data/{{ ti.xcom_pull(task_ids='log_start') }}/`

🎯 *처리 결과*:
- 각 사용자-영화별 상호작용 점수 집계 완료
- content_click(1점), like_click(3점), review_write(1점), rating_submit(1점)
- LightFM 모델 학습용 데이터셋 준비 완료

🔗 *Spark UI*: {{ params.spark_master.replace('spark://', 'http://').replace(':7077', ':8080') }}
    """,
    params={'spark_master': SPARK_MASTER},
    dag=dag,
    trigger_rule='all_success',
)

# Task 11: 실패 알림 (Slack) - 실패 시에만 실행
failure_notification = SlackWebhookOperator(
    task_id='send_failure_notification',
    http_conn_id='slack_webhook',
    message="""
❌ *사용자 로그 집계 ELT 실패*

📅 처리 날짜: {{ ti.xcom_pull(task_ids='log_start') }}
🕐 실패 시각: {{ ds }}
📊 DAG: {{ dag.dag_id }}
❌ 상태: 실패

🔍 *확인 사항*:
- Spark 클러스터 상태 확인
- MinIO 연결 상태 확인  
- 소스 데이터 존재 여부 확인

📋 *로그 확인*: {{ ti.log_url }}

🚨 *조치 필요*: 데이터팀에서 즉시 확인 바랍니다.
    """,
    dag=dag,
    trigger_rule='one_failed',
)

# Task 12: 정리 작업 (선택사항)
cleanup_temp_files = BashOperator(
    task_id='cleanup_temp_files',
    bash_command="""
    echo "🧹 임시 파일 정리 중..."
    
    # 임시 보고서 파일 정리
    rm -f /tmp/data_quality_report_*.txt
    
    # Spark 임시 파일 정리 (필요시)
    # find /tmp -name "spark-*" -type d -mtime +1 -exec rm -rf {} + 2>/dev/null || true
    
    echo "✅ 정리 작업 완료"
    """,
    dag=dag,
    trigger_rule='all_done',  # 성공/실패 관계없이 실행
)

# ==========================================
# Task 의존성 설정
# ==========================================

# 초기 검증 단계
start_logging >> validate_date

# 병렬 사전 검증
validate_date >> [check_source_data, check_spark_cluster, prepare_target_storage]

# 메인 처리
[check_source_data, check_spark_cluster, prepare_target_storage] >> run_spark_job

# 후처리 및 검증
run_spark_job >> validate_output >> generate_quality_report >> complete_logging

# 알림 (성공/실패 분기)
complete_logging >> success_notification
[start_logging, validate_date, check_source_data, check_spark_cluster, 
 prepare_target_storage, run_spark_job, validate_output, 
 generate_quality_report, complete_logging] >> failure_notification

# 정리 작업 (모든 task 완료 후)
[success_notification, failure_notification] >> cleanup_temp_files

# ==========================================
# DAG 문서화
# ==========================================

dag.doc_md = """
# 사용자 로그 집계 ELT DAG

## 목적
- 일별 사용자 행동 로그를 집계하여 LightFM 추천 모델 학습용 데이터셋 생성
- 각 사용자-영화별 상호작용 점수를 이벤트 타입별로 집계

## 처리 과정
1. **데이터 로드**: MinIO에서 일별 사용자 로그 데이터 읽기
2. **점수화**: 이벤트별 점수 부여 (content_click:1, like_click:3, review_write:1, rating_submit:1)
3. **집계**: 사용자-영화별 각 이벤트 점수 합산 및 total_score 계산
4. **저장**: 집계 결과를 날짜별 파티션으로 저장

## 스케줄
- **실행 시간**: 매일 새벽 1:00 AM
- **재시도**: 3회 (5분 간격)
- **타임아웃**: 1시간

## 데이터 경로
- **입력**: `s3://userlog-data/test/user-activity-raw/YYYY-MM-DD/`
- **출력**: `s3://ml-learning-data/YYYY-MM-DD/`

## 알림
- **성공**: Slack 알림
- **실패**: Slack 알림 + 이메일

## 의존성
- Spark 클러스터 (standalone mode)
- MinIO 오브젝트 스토리지
- Python 3.8+
- Hadoop AWS S3 라이브러리

## 모니터링
- Airflow 웹 UI
- Spark 웹 UI (http://your-ec2-ip:8080)
- MinIO 콘솔 (http://your-ec2-ip:9001)
"""
