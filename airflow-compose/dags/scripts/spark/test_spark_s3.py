#!/usr/bin/env python3
"""
S3 Parquet 데이터 테스트용 Spark 처리 스크립트
목적: AWS EC2 Spark 클러스터에서 S3 데이터 읽기/처리/저장 테스트

테스트 데이터: s3://goorm-customer-log/userlog-activity-raw/uniform-test/user_activity_logs_uniform_dask_20250623_165506.parquet
결과 저장: s3://goorm-customer-log/ml-learning-data/test-results/

실행 방법:
spark-submit --master spark://10.0.20.54:7077 \
    --packages org.apache.hadoop:hadoop-aws:3.3.4 \
    test_spark_s3.py
"""

import sys
import time
import logging
from datetime import datetime
from typing import Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, count, when, lit,
    current_timestamp, desc, avg, max as spark_max
)
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JJavaError

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SparkConfig:
    """Spark 세션 설정 관리"""
    
    @staticmethod
    def get_base_config() -> Dict[str, str]:
        """기본 Spark 설정"""
        return {
            # === 애플리케이션 설정 ===
            "spark.app.name": "S3-Parquet-Test",
            
            # === SQL 최적화 설정 ===
            "spark.sql.adaptive.enabled": "true",                    # 적응형 쿼리 실행 활성화
            "spark.sql.adaptive.coalescePartitions.enabled": "true", # 파티션 자동 병합
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB", # 권장 파티션 크기
            
            # === 시리얼라이저 설정 ===
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer", # 빠른 직렬화
            
            # === Arrow 최적화 ===
            "spark.sql.execution.arrow.pyspark.enabled": "true",     # Pandas 변환 최적화
            
            # === 메모리 관리 ===
            "spark.sql.execution.arrow.maxRecordsPerBatch": "10000", # Arrow 배치 크기
        }
    
    @staticmethod 
    def get_s3_config(region: str = "ap-northeast-2") -> Dict[str, str]:
        """S3 연동 설정"""
        return {
            # === S3A 파일시스템 설정 ===
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            
            # === Parquet 호환성 설정 ===
            "spark.sql.parquet.int96RebaseModeInRead": "CORRECTED",
            "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED", 
            "spark.sql.parquet.datetimeRebaseModeInRead": "CORRECTED",
            "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
            "spark.sql.parquet.outputTimestampType": "TIMESTAMP_MICROS",  # nanoseconds 대신 microseconds 사용
            
            # === 인증 설정 ===
            "spark.hadoop.fs.s3a.aws.credentials.provider": 
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain", # EC2 IAM 역할 사용
            
            # === 성능 최적화 설정 ===
            "spark.hadoop.fs.s3a.block.size": "134217728",           # 128MB 블록 크기
            "spark.hadoop.fs.s3a.buffer.dir": "/tmp",                # 로컬 버퍼 디렉토리
            "spark.hadoop.fs.s3a.fast.upload": "true",               # 빠른 업로드 모드
            "spark.hadoop.fs.s3a.multipart.size": "104857600",       # 100MB 멀티파트 크기
            "spark.hadoop.fs.s3a.multipart.threshold": "67108864",   # 64MB 멀티파트 임계값
            "spark.hadoop.fs.s3a.threads.max": "10",                 # 최대 연결 스레드 수
            
            # === 연결 설정 ===
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",    # SSL 암호화 활성화
            "spark.hadoop.fs.s3a.endpoint": f"s3.{region}.amazonaws.com", # 리전별 엔드포인트
            "spark.hadoop.fs.s3a.path.style.access": "false",        # Virtual-hosted 스타일 사용
            
            # === 재시도 및 타임아웃 설정 ===
            "spark.hadoop.fs.s3a.retry.limit": "3",                  # 재시도 횟수
            "spark.hadoop.fs.s3a.connection.timeout": "200000",      # 연결 타임아웃 (200초)
        }
    
    @classmethod
    def get_complete_config(cls, region: str = "ap-northeast-2") -> Dict[str, str]:
        """전체 Spark 설정 반환"""
        config = {}
        config.update(cls.get_base_config())
        config.update(cls.get_s3_config(region))
        return config

class S3Config:
    """AWS S3 연결 설정"""
    
    # === S3 버킷 및 경로 설정 ===
    BUCKET = "goorm-customer-log"
    SOURCE_PATH = "userlog-activity-raw/uniform-test/"
    TEST_FILE = "user_activity_logs_uniform_dask_20250623_165506.parquet"
    TARGET_PATH = "ml-learning-data/test-results/"

class SparkS3Tester:
    """Spark S3 연동 테스트 클래스"""
    
    def __init__(self):
        """초기화"""
        self.spark = None
        self.s3_config = S3Config()
        self.start_time = time.time()
        
        # 처리 결과 저장용
        self.results = {
            'step_times': {},      # 각 단계별 실행 시간
            'data_stats': {},      # 데이터 통계 정보
            'success': False       # 전체 실행 성공 여부
        }
    
    def _log_step_time(self, step_name: str) -> None:
        """단계별 처리 시간 기록"""
        current_time = time.time()
        elapsed = current_time - self.start_time
        self.results['step_times'][step_name] = elapsed
        logger.info(f"⏱️ {step_name}: {elapsed:.2f}초")
        self.start_time = current_time
    
    def create_spark_session(self) -> SparkSession:
        """Spark 세션 생성 및 설정"""
        logger.info("🚀 Spark 세션 생성 중...")
        
        try:
            # === Spark 설정 구성 ===
            spark_config = SparkConfig.get_complete_config()
            
            # === Spark 세션 빌더 생성 ===
            builder = SparkSession.builder
            
            # === 설정 적용 ===
            for key, value in spark_config.items():
                builder = builder.config(key, value)
                logger.debug(f"  📋 {key}: {value}")
            
            # === 세션 생성 ===
            spark = builder.getOrCreate()
            
            # === 로그 레벨 설정 (Spark 내부 로그 최소화) ===
            spark.sparkContext.setLogLevel("WARN")
            
            # === 세션 정보 출력 ===
            logger.info("✅ Spark 세션 생성 완료")
            logger.info(f"📊 Spark 버전: {spark.version}")
            logger.info(f"🌐 클러스터 모드: {spark.conf.get('spark.master', 'local')}")
            logger.info(f"🎯 애플리케이션 이름: {spark.conf.get('spark.app.name')}")
            
            # === 클러스터 연결 정보 확인 ===
            sc = spark.sparkContext
            logger.info(f"🔗 Master URL: {sc.master}")
            logger.info(f"🆔 Application ID: {sc.applicationId}")
            logger.info(f"💾 기본 병렬도: {sc.defaultParallelism}")
            
            self.spark = spark
            self._log_step_time("Spark 세션 생성")
            return spark
            
        except Exception as e:
            logger.error(f"❌ Spark 세션 생성 실패: {e}")
            raise
    
    def test_s3_connection(self) -> bool:
        """S3 연결 테스트 (TIMESTAMP(NANOS) 문제 우회 및 상세 디버깅)"""
        logger.info("🔍 S3 연결 테스트 시작...")
        logger.info("🛠️ S3 경로 존재 확인 중...")
        
        # 실제 사용할 S3 버킷으로 연결 테스트
        test_path = f"s3a://{self.s3_config.BUCKET}/"
        parquet_path = f"s3a://{self.s3_config.BUCKET}/{self.s3_config.SOURCE_PATH}*.parquet"
        
        logger.info(f"🔍 디버깅: 테스트할 S3 경로: {parquet_path}")
        
        try:
            # 방법 1: 일반 읽기 시도
            logger.info("🔄 Parquet 스키마 읽기 시도 중...")
            test_df = self.spark.read \
                .option("mergeSchema", "false") \
                .option("spark.sql.parquet.enableVectorizedReader", "false") \
                .parquet(parquet_path) \
                .limit(0)
            
            logger.info(f"✅ S3 연결 테스트 성공: {self.s3_config.BUCKET}")
            logger.info(f"🔗 테스트 경로: {test_path}")
            
        except Exception as e:
            # 상세한 예외 정보 수집
            exception_type = type(e).__name__
            exception_module = type(e).__module__
            error_msg = str(e)
            
            logger.error(f"🐛 예외 발생 상세 정보:")
            logger.error(f"   📝 예외 타입: {exception_type}")
            logger.error(f"   📦 예외 모듈: {exception_module}")
            logger.error(f"   📄 에러 메시지 길이: {len(error_msg)} 문자")
            logger.error(f"   📄 에러 메시지 첫 200자: {error_msg[:200]}...")
            
            # 스택 트레이스도 출력
            import traceback
            logger.error(f"   🔍 스택 트레이스:")
            for line in traceback.format_exc().split('\n')[:10]:  # 첫 10줄만
                if line.strip():
                    logger.error(f"     {line}")
            
            # TIMESTAMP(NANOS) 에러 패턴 확인
            timestamp_patterns = [
                "Illegal Parquet type",
                "TIMESTAMP(NANOS",
                "TIMESTAMP(NANOS,false)",
                "INT64 (TIMESTAMP(NANOS"
            ]
            
            pattern_found = []
            for pattern in timestamp_patterns:
                if pattern in error_msg:
                    pattern_found.append(pattern)
            
            if pattern_found:
                logger.warning(f"⚠️ TIMESTAMP(NANOS) 관련 패턴 감지: {pattern_found}")
                logger.info(f"✅ S3 연결 테스트 성공 (타임스탬프 호환성 문제 있음): {self.s3_config.BUCKET}")
                logger.info(f"🔗 테스트 경로: {test_path}")
                logger.info("📋 다음 단계에서 STRING 변환 로직을 사용합니다.")
                # S3 연결 자체는 성공으로 간주
            elif "NoSuchBucket" in error_msg:
                logger.error(f"❌ S3 버킷을 찾을 수 없음: {self.s3_config.BUCKET}")
                return False
            elif "AccessDenied" in error_msg or "403" in error_msg:
                logger.error(f"❌ S3 버킷 접근 권한 없음: {self.s3_config.BUCKET}")
                logger.error("🔧 IAM 역할의 S3 읽기 권한을 확인해주세요.")
                return False
            elif "NoSuchKey" in error_msg or "404" in error_msg:
                logger.error(f"❌ S3 파일을 찾을 수 없음: {parquet_path}")
                logger.error("🔍 파일 경로와 이름을 확인해주세요.")
                return False
            else:
                logger.error(f"❌ 알 수 없는 S3 연결 오류")
                logger.error(f"🔍 추가 분석이 필요합니다.")
                return False
        
        self._log_step_time("S3 연결 테스트")
        return True
    
    def load_test_data(self) -> DataFrame:
        """테스트 Parquet 파일 로드 (TIMESTAMP(NANOS) 문제 해결)"""
        logger.info("📂 테스트 데이터 로드 시작...")
        
        file_path = f"s3a://{self.s3_config.BUCKET}/{self.s3_config.SOURCE_PATH}{self.s3_config.TEST_FILE}"
        logger.info(f"🔍 로드할 파일: {file_path}")
        
        try:
            # 방법 1: 일반 읽기 시도
            logger.info("🛠️ 일반 모드로 읽기 시도...")
            df = self.spark.read \
                .option("mergeSchema", "false") \
                .option("spark.sql.parquet.enableVectorizedReader", "false") \
                .parquet(file_path)
            logger.info("✅ 일반 모드 읽기 성공!")

        except AnalysisException as e:
            error_msg = str(e)
            if "Illegal Parquet type" in error_msg and "TIMESTAMP(NANOS" in error_msg:
                logger.warning("⚠️ TIMESTAMP(NANOS) 문제 감지 - STRING 변환 모드로 전환")
                logger.info("🔄 타임스탬프 컬럼을 STRING으로 읽기...")
                
                # 스키마를 수동으로 정의하여 타임스탬프를 문자열로 처리
                from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType
                
                # generate_dummy_log.py와 일치하는 스키마
                manual_schema = StructType([
                    StructField("userId", StringType(), True),
                    StructField("title", StringType(), True),
                    StructField("videoId", StringType(), True),
                    StructField("timestamp", StringType(), True),  # TIMESTAMP(NANOS) -> STRING
                    StructField("eventType", StringType(), True),
                    StructField("genre", ArrayType(StringType()), True),
                    StructField("page", StringType(), True),
                    StructField("rating", IntegerType(), True),
                    StructField("review", StringType(), True),
                    StructField("liked", BooleanType(), True),
                    StructField("recMovieList", ArrayType(StringType()), True)
                ])
                
                logger.info("📋 수동 스키마로 읽기 (타임스탬프 → STRING 변환)")
                df = self.spark.read \
                    .schema(manual_schema) \
                    .option("mergeSchema", "false") \
                    .option("spark.sql.parquet.enableVectorizedReader", "false") \
                    .parquet(file_path)
                
                logger.info("✅ STRING 모드 읽기 성공!")
                logger.info("💡 타임스탬프 컬럼들이 문자열로 로드되었습니다.")
            elif "Illegal Parquet type" in error_msg:
                logger.error("❌ [치명적] 지원하지 않는 Parquet 타입입니다.")
                logger.error("🛠 해결 방법: parquet 파일을 재작성하거나 Spark 버전을 변경해보세요.")
                logger.error(f"📁 파일 경로: {file_path}")
                raise
            else:
                logger.error(f"❌ Parquet 스키마 오류: {e}")
                logger.error(f"📁 파일 경로 확인: {file_path}")
                raise
        except Py4JJavaError as e:
            if "NoSuchKey" in str(e) or "404" in str(e):
                logger.error(f"❌ S3 파일을 찾을 수 없음: {file_path}")
                logger.error("🔍 S3 버킷과 경로를 확인해주세요.")
            else:
                logger.error(f"❌ S3 연결 오류: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ 데이터 로드 중 알 수 없는 오류: {e}")
            raise
            
            # DataFrame 캐싱 (성능 최적화)
            df = df.cache()
            
            # 기본 정보 확인 (한 번만 count 호출)
            row_count = df.count()
            column_count = len(df.columns)
            
            logger.info(f"✅ 데이터 로드 성공!")
            logger.info(f"📊 레코드 수: {row_count:,}개")
            logger.info(f"📋 컬럼 수: {column_count}개")
            
            # 데이터 검증
            self._validate_dataframe_schema(df)
            
            # 스키마 정보 출력
            logger.info("📝 데이터 스키마:")
            df.printSchema()
            
            # 컬럼명 확인
            logger.info(f"📂 컬럼명: {df.columns}")
            
            # 첫 5개 레코드 미리보기
            logger.info("👀 데이터 미리보기 (5개 레코드):")
            df.show(5, truncate=False)
            
            # 통계 저장
            self.results['data_stats'] = {
                'total_rows': row_count,
                'total_columns': column_count,
                'columns': df.columns,
                'file_path': file_path
            }
            
            self._log_step_time("데이터 로드")
            return df
    
    def _validate_dataframe_schema(self, df: DataFrame) -> None:
        """DataFrame 스키마 검증"""
        required_columns = ['userId', 'videoId', 'title', 'eventType']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"❌ 필수 컬럼 누락: {missing_columns}")
        
        # 빈 데이터프레임 체크
        if df.count() == 0:
            raise ValueError("❌ 빈 데이터프레임입니다.")
        
        logger.info("✅ 스키마 검증 완료")
    
    def analyze_data(self, df: DataFrame) -> DataFrame:
        """데이터 분석 및 userlog_agg_pyspark.py와 동일한 집계 로직 수행"""
        logger.info("📈 데이터 분석 및 집계 시작...")
        
        try:
            # 컬럼이 있는지 확인 후 분석
            columns = df.columns
            
            # 기본 통계
            logger.info("📊 기본 데이터 분석:")
            
            # 1. 각 컬럼별 null 값 확인 (성능 최적화)
            logger.info("🔍 NULL 값 분석:")
            
            # 한 번에 모든 컬럼의 null count 계산 (성능 개선)
            null_counts = df.select([
                spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) 
                for c in columns
            ]).collect()[0]
            
            total_count = df.count()  # 캐시된 DataFrame이므로 빠름
            
            for col_name in columns:
                null_count = null_counts[col_name]
                null_percentage = (null_count / total_count) * 100 if total_count > 0 else 0
                logger.info(f"  {col_name}: {null_count:,}개 ({null_percentage:.1f}%)")
            
            # 2. 기본 데이터 통계
            if 'userId' in columns:
                unique_users = df.select('userId').distinct().count()
                logger.info(f"👥 고유 사용자 수: {unique_users:,}명")
            
            if 'eventType' in columns:
                # 이벤트 타입별 분포
                event_dist = df.groupBy('eventType').count().orderBy(desc('count'))
                logger.info("📈 이벤트 타입별 분포:")
                event_dist.show()
            
            if 'videoId' in columns:
                unique_videos = df.select('videoId').distinct().count()
                logger.info(f"🎬 고유 비디오 수: {unique_videos:,}개")
            
            # ==========================================
            # 3. userlog_agg_pyspark.py와 동일한 집계 로직 수행
            # ==========================================
            
            logger.info("🎯 이벤트별 점수 적용 중...")
            
            # 각 이벤트 타입별 개별 점수 컬럼 생성 (체이닝으로 최적화)
            scored_df = df \
                .withColumn("content_click_score", when(col("eventType") == "content_click", 1).otherwise(0)) \
                .withColumn("like_click_score", when(col("eventType") == "like_click", 3).otherwise(0)) \
                .withColumn("review_write_score", when(col("eventType") == "review_write", 1).otherwise(0)) \
                .withColumn("rating_submit_score", when(col("eventType") == "rating_submit", 1).otherwise(0)) \
                .cache()  # 중간 결과 캐싱
            
            # 점수별 통계 출력 (한 번에 계산)
            logger.info("📈 이벤트별 점수 분포:")
            score_stats = scored_df.agg(
                spark_sum(when(col("content_click_score") > 0, 1).otherwise(0)).alias("content_clicks"),
                spark_sum(when(col("like_click_score") > 0, 1).otherwise(0)).alias("like_clicks"),
                spark_sum(when(col("review_write_score") > 0, 1).otherwise(0)).alias("reviews"),
                spark_sum(when(col("rating_submit_score") > 0, 1).otherwise(0)).alias("ratings")
            ).collect()[0]
            
            logger.info(f"  Content Click: {score_stats['content_clicks']:,}개 (총 {score_stats['content_clicks']}점)")
            logger.info(f"  Like Click: {score_stats['like_clicks']:,}개 (총 {score_stats['like_clicks'] * 3}점)")
            logger.info(f"  Review Write: {score_stats['reviews']:,}개 (총 {score_stats['reviews']}점)")
            logger.info(f"  Rating Submit: {score_stats['ratings']:,}개 (총 {score_stats['ratings']}점)")
            
            logger.info("📊 사용자별, 영화별 점수 집계 중...")
            
            # 사용자별, 비디오별 점수 집계 (userlog_agg_pyspark.py와 동일)
            aggregated_df = scored_df.groupBy("userId", "videoId", "title") \
                .agg(
                    # 각 이벤트별 점수 합계
                    spark_sum("content_click_score").alias("content_click_total"),
                    spark_sum("like_click_score").alias("like_click_total"),
                    spark_sum("review_write_score").alias("review_write_total"),
                    spark_sum("rating_submit_score").alias("rating_submit_total"),
                    
                    # 추가 메트릭
                    count("*").alias("interaction_count"),
                    current_timestamp().alias("processed_at")
                ) \
                .withColumn(
                    "total_score",
                    col("content_click_total") + 
                    col("like_click_total") + 
                    col("review_write_total") + 
                    col("rating_submit_total")
                ) \
                .filter(col("total_score") > 0) \
                .cache()  # 최종 결과 캐싱
            
            # 집계 결과 통계
            total_interactions = aggregated_df.count()
            logger.info(f"✅ 집계 완료: {total_interactions:,}개 사용자-영화 상호작용")
            
            # 전체 점수 분포 확인
            score_dist = aggregated_df.groupBy("total_score").count().orderBy("total_score").limit(10).collect()
            logger.info("📊 전체 점수 분포 (Top 10):")
            for row in score_dist:
                logger.info(f"  점수 {row['total_score']}: {row['count']:,}개")
            
            # 이벤트별 점수 기여도 분석
            total_stats = aggregated_df.agg(
                spark_sum("content_click_total").alias("total_content_clicks"),
                spark_sum("like_click_total").alias("total_like_clicks"),
                spark_sum("review_write_total").alias("total_reviews"),
                spark_sum("rating_submit_total").alias("total_ratings"),
                spark_sum("total_score").alias("grand_total")
            ).collect()[0]
            
            logger.info("📈 이벤트별 점수 기여도:")
            grand_total = total_stats['grand_total']
            if grand_total > 0:
                content_pct = (total_stats['total_content_clicks'] / grand_total) * 100
                like_pct = (total_stats['total_like_clicks'] / grand_total) * 100
                review_pct = (total_stats['total_reviews'] / grand_total) * 100
                rating_pct = (total_stats['total_ratings'] / grand_total) * 100
                
                logger.info(f"  Content Click: {total_stats['total_content_clicks']:,}점 ({content_pct:.1f}%)")
                logger.info(f"  Like Click: {total_stats['total_like_clicks']:,}점 ({like_pct:.1f}%)")
                logger.info(f"  Review Write: {total_stats['total_reviews']:,}점 ({review_pct:.1f}%)")
                logger.info(f"  Rating Submit: {total_stats['total_ratings']:,}점 ({rating_pct:.1f}%)")
                logger.info(f"  총 합계: {grand_total:,}점")
            
            # 상위 활발한 사용자 확인
            top_users = aggregated_df.groupBy("userId") \
                .agg(spark_sum("total_score").alias("user_total_score")) \
                .orderBy(col("user_total_score").desc()) \
                .limit(5).collect()
            
            logger.info("👥 상위 활발한 사용자 5명:")
            for i, row in enumerate(top_users, 1):
                logger.info(f"  {i}. User: {row['userId'][:8]}**** (총점: {row['user_total_score']})")
            
            # LightFM 학습용 데이터 예시 출력
            logger.info("📄 LightFM 학습용 데이터 예시 (5건):")
            sample_data = aggregated_df.select(
                "userId", "videoId", "title", 
                "content_click_total", "like_click_total", 
                "review_write_total", "rating_submit_total", 
                "total_score", "interaction_count"
            ).limit(5).collect()
            
            for i, row in enumerate(sample_data, 1):
                logger.info(f"  {i}. User: {row['userId'][:8]}**** | Video: {row['videoId']} | Title: {row['title']}")
                logger.info(f"     Content:{row['content_click_total']} Like:{row['like_click_total']} Review:{row['review_write_total']} Rating:{row['rating_submit_total']} Total:{row['total_score']}")
            
            self._log_step_time("데이터 분석 및 집계")
            return aggregated_df
            
        except AnalysisException as e:
            logger.error(f"❌ 데이터 집계 중 스키마 오류: {e}")
            raise
        except Py4JJavaError as e:
            logger.error(f"❌ Spark 연산 중 Java 오류: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ 데이터 분석 실패: {e}")
            raise
        finally:
            # 중간 DataFrame 캐시 해제 (메모리 정리)
            if 'scored_df' in locals():
                scored_df.unpersist()
    
    def save_results(self, df: DataFrame) -> None:
        """분석 결과를 S3에 저장 (userlog_agg_pyspark.py와 동일한 방식)"""
        logger.info("💾 결과 저장 시작...")
        
        # 저장 경로 생성 (날짜별 파티셔닝)
        process_date = datetime.now().strftime('%Y-%m-%d')
        save_path = f"s3a://{self.s3_config.BUCKET}/{self.s3_config.TARGET_PATH}"
        
        logger.info(f"📁 저장 경로: {save_path}")
        
        try:
            # userlog_agg_pyspark.py와 동일하게 process_date 컬럼 추가
            df_with_date = df.withColumn("process_date", lit(process_date))
            
            # Parquet으로 저장 (날짜별 파티셔닝)
            df_with_date.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .partitionBy("process_date") \
                .parquet(save_path)
            
            saved_count = df.count()
            logger.info(f"✅ 저장 완료: {saved_count:,}개 레코드")
            logger.info(f"📂 저장 위치: {save_path}process_date={process_date}/")
            
            # 결과에 저장 정보 추가
            self.results['data_stats']['output_path'] = f"{save_path}process_date={process_date}/"
            self.results['data_stats']['output_records'] = saved_count
            self.results['data_stats']['process_date'] = process_date
            
            logger.info("📊 저장된 데이터 정보:")
            logger.info(f"  처리 날짜: {process_date}")
            logger.info(f"  파티션 경로: process_date={process_date}")
            logger.info(f"  압축 방식: Snappy")
            logger.info(f"  파일 형식: Parquet")
            
            self._log_step_time("결과 저장")
            
        except AnalysisException as e:
            logger.error(f"❌ Parquet 저장 중 스키마 오류: {e}")
            raise
        except Py4JJavaError as e:
            if "AccessDenied" in str(e):
                logger.error(f"❌ S3 쓰기 권한 없음: {save_path}")
                logger.error("🔧 IAM 역할의 S3 쓰기 권한을 확인해주세요.")
            else:
                logger.error(f"❌ S3 저장 중 오류: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ 결과 저장 실패: {e}")
            raise
    
    def test_cluster_info(self) -> None:
        """클러스터 정보 확인"""
        logger.info("🖥️ 클러스터 정보 확인...")
        
        try:
            # Spark 컨텍스트 정보
            sc = self.spark.sparkContext
            
            logger.info("📊 클러스터 정보:")
            logger.info(f"  🎯 Master: {sc.master}")
            logger.info(f"  📱 App Name: {sc.appName}")
            logger.info(f"  🆔 App ID: {sc.applicationId}")
            logger.info(f"  💾 Default Parallelism: {sc.defaultParallelism}")
            
            # Worker 정보 (가능한 경우)
            try:
                # 간단한 RDD 작업으로 Worker 확인
                rdd = sc.parallelize(range(100), 4)
                partitions = rdd.getNumPartitions()
                result = rdd.map(lambda x: x * 2).count()
                
                logger.info(f"  🔧 파티션 수: {partitions}")
                logger.info(f"  ✅ 테스트 연산 결과: {result}")
                
            except Exception as e:
                logger.warning(f"⚠️ Worker 테스트 실패: {e}")
            
            self._log_step_time("클러스터 정보 확인")
            
        except Exception as e:
            logger.error(f"❌ 클러스터 정보 확인 실패: {e}")
    
    def cleanup(self) -> None:
        """리소스 정리"""
        if self.spark:
            logger.info("🧹 Spark 세션 정리 중...")
            
            # 캐시된 DataFrame들 정리
            try:
                self.spark.catalog.clearCache()
                logger.info("✅ DataFrame 캐시 정리 완료")
            except Exception as e:
                logger.warning(f"⚠️ 캐시 정리 중 오류: {e}")
            
            # Spark 세션 종료
            try:
                self.spark.stop()
                logger.info("✅ Spark 세션 정리 완료")
            except Exception as e:
                logger.warning(f"⚠️ Spark 세션 종료 중 오류: {e}")
    
    def print_test_summary(self) -> None:
        """테스트 결과 요약 출력"""
        logger.info("\n" + "=" * 70)
        logger.info("🎯 SPARK CLUSTER S3 테스트 결과 요약")
        logger.info("=" * 70)
        
        # 전체 소요 시간
        total_time = sum(self.results['step_times'].values())
        logger.info(f"⏱️ 총 소요 시간: {total_time:.2f}초 ({total_time/60:.1f}분)")
        
        # 단계별 시간
        logger.info("\n📊 단계별 처리 시간:")
        for step, elapsed in self.results['step_times'].items():
            percentage = (elapsed / total_time) * 100 if total_time > 0 else 0
            logger.info(f"  {step}: {elapsed:.2f}초 ({percentage:.1f}%)")
        
        # 데이터 통계
        if self.results['data_stats']:
            stats = self.results['data_stats']
            logger.info(f"\n📈 데이터 처리 결과:")
            logger.info(f"  입력 레코드: {stats.get('total_rows', 'N/A'):,}개")
            logger.info(f"  입력 컬럼: {stats.get('total_columns', 'N/A')}개")
            logger.info(f"  출력 레코드: {stats.get('output_records', 'N/A'):,}개")
            logger.info(f"  출력 경로: {stats.get('output_path', 'N/A')}")
        
        logger.info(f"\n✅ 테스트 상태: {'성공' if self.results['success'] else '실패'}")
        logger.info(f"🕐 완료 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)
    
    def run_full_test(self) -> bool:
        """전체 테스트 실행"""
        logger.info("🎬 Spark Cluster S3 테스트 시작!")
        logger.info(f"📅 실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # 1. Spark 세션 생성
            self.create_spark_session()
            
            # 2. 클러스터 정보 확인
            self.test_cluster_info()
            
            # 3. S3 연결 테스트
            if not self.test_s3_connection():
                raise Exception("S3 연결 실패")
            
            # 4. 테스트 데이터 로드
            df = self.load_test_data()
            
            # 5. 데이터 분석
            result_df = self.analyze_data(df)
            
            # 6. 결과 저장
            self.save_results(result_df)
            
            # 성공 플래그 설정
            self.results['success'] = True
            
            logger.info("🎉 모든 테스트 성공적으로 완료!")
            return True
            
        except Exception as e:
            logger.error(f"❌ 테스트 실행 중 오류: {e}")
            self.results['success'] = False
            return False
            
        finally:
            # 결과 요약 출력
            self.print_test_summary()
            
            # 리소스 정리
            self.cleanup()

def main():
    """메인 함수"""
    logger.info("🚀 Spark Cluster S3 연동 테스트 시작")
    
    try:
        # 테스터 생성 및 실행
        tester = SparkS3Tester()
        success = tester.run_full_test()
        
        # 종료 코드 설정
        sys.exit(0 if success else 1)
        
    except Exception as e:
        logger.error(f"❌ 프로그램 실행 실패: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
