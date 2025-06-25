#!/usr/bin/env python3
"""
고객 로그 기반 상호작용 점수 집계 Spark ELT 처리
목적: LightFM 모델 학습용 데이터셋 생성

처리 과정:
1. MinIO에서 일별 고객 로그 데이터 불러오기
2. 이벤트별 점수화 (content_click:1, like_click:3, review_write:1, rating_submit:1)
3. 각 이벤트별 개별 점수 + total_score 집계
4. 결과를 MinIO에 저장

실행 방법:
spark-submit --master spark://your-ec2-ip:7077 userlog_agg_pyspark.py
"""

import sys
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, count, when, lit,
    current_timestamp, date_format, regexp_extract
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    TimestampType, BooleanType, ArrayType
)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MinIOConfig:
    """MinIO 연결 설정"""
    ENDPOINT = "http://your-ec2-ip:9000"  # EC2 MinIO 엔드포인트
    ACCESS_KEY = "minioadmin"
    SECRET_KEY = "minioadmin"
    
    # 데이터 경로
    SOURCE_BUCKET = "userlog-data"
    TARGET_BUCKET = "ml-learning-data"
    
    @classmethod
    def get_s3_config(cls) -> Dict[str, str]:
        """Spark용 S3 설정 반환"""
        return {
            "spark.hadoop.fs.s3a.endpoint": cls.ENDPOINT,
            "spark.hadoop.fs.s3a.access.key": cls.ACCESS_KEY,
            "spark.hadoop.fs.s3a.secret.key": cls.SECRET_KEY,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
        }

class EventScoreConfig:
    """이벤트별 점수 설정"""
    SCORE_MAPPING = {
        'content_click': 1,
        'like_click': 3,
        'review_write': 1,
        'rating_submit': 1,
        'play_start': 0,      # 점수 없음
        'play_stop': 0,       # 점수 없음
        'content_recom_click': 0  # 점수 없음
    }

class UserLogELTProcessor:
    """사용자 로그 ELT 처리 클래스"""
    
    def __init__(self, process_date: str = None):
        """
        초기화
        
        Args:
            process_date: 처리할 날짜 (YYYY-MM-DD), None이면 어제 날짜
        """
        self.process_date = process_date or self._get_yesterday()
        self.spark = None
        self.minio_config = MinIOConfig()
        self.score_config = EventScoreConfig()
        
        # 처리 시간 측정용
        self.start_time = None
        self.step_times = {}
    
    def _get_yesterday(self) -> str:
        """어제 날짜를 YYYY-MM-DD 형식으로 반환"""
        yesterday = datetime.now() - timedelta(days=1)
        return yesterday.strftime('%Y-%m-%d')
    
    def _log_step_time(self, step_name: str) -> None:
        """단계별 처리 시간 기록"""
        current_time = time.time()
        if self.start_time:
            elapsed = current_time - self.start_time
            self.step_times[step_name] = elapsed
            logger.info(f"⏱️ {step_name} 처리 시간: {elapsed:.2f}초")
        self.start_time = current_time
    
    def create_spark_session(self) -> SparkSession:
        """Spark 세션 생성"""
        logger.info("🚀 Spark 세션 생성 중...")
        
        # Spark 설정
        spark_config = {
            "spark.app.name": f"UserLogAggregation-{self.process_date}",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            **self.minio_config.get_s3_config()
        }
        
        # Spark 세션 빌더
        builder = SparkSession.builder
        
        # 설정 적용
        for key, value in spark_config.items():
            builder = builder.config(key, value)
        
        # 세션 생성
        spark = builder.getOrCreate()
        
        # 로그 레벨 설정
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info("✅ Spark 세션 생성 완료")
        logger.info(f"📊 Spark 버전: {spark.version}")
        logger.info(f"🎯 처리 날짜: {self.process_date}")
        
        self.spark = spark
        return spark
    
    def define_schema(self) -> StructType:
        """사용자 로그 스키마 정의"""
        return StructType([
            StructField("userId", StringType(), True),
            StructField("title", StringType(), True),
            StructField("videoId", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("eventType", StringType(), True),
            StructField("genre", ArrayType(StringType()), True),
            StructField("page", StringType(), True),
            StructField("rating", IntegerType(), True),
            StructField("review", StringType(), True),
            StructField("liked", BooleanType(), True),
            StructField("recMovieList", ArrayType(StringType()), True)
        ])
    
    def load_data(self) -> DataFrame:
        """MinIO에서 데이터 불러오기"""
        logger.info(f"📂 데이터 불러오기 시작: {self.process_date}")
        
        # S3 경로 구성
        source_path = f"s3a://{self.minio_config.SOURCE_BUCKET}/test/user-activity-raw/{self.process_date}/"
        
        logger.info(f"🔍 데이터 경로: {source_path}")
        
        try:
            # 스키마 정의
            schema = self.define_schema()
            
            # Parquet 파일 읽기
            df = self.spark.read \
                .schema(schema) \
                .option("mergeSchema", "true") \
                .parquet(source_path)
            
            # 데이터 검증
            row_count = df.count()
            logger.info(f"✅ 데이터 로드 완료: {row_count:,}개 레코드")
            
            # 기본 통계 출력
            logger.info("📊 데이터 기본 정보:")
            df.printSchema()
            
            # 이벤트 타입별 분포 확인
            event_dist = df.groupBy("eventType").count().collect()
            logger.info("📈 이벤트 타입별 분포:")
            for row in event_dist:
                logger.info(f"  {row['eventType']}: {row['count']:,}개")
            
            self._log_step_time("데이터 로드")
            return df
            
        except Exception as e:
            logger.error(f"❌ 데이터 로드 실패: {e}")
            raise
    
    def apply_scoring(self, df: DataFrame) -> DataFrame:
        """이벤트별 점수 적용 - 각 이벤트 타입별 개별 점수 컬럼만 생성"""
        logger.info("🎯 이벤트별 점수 적용 시작...")
        
        # 각 이벤트 타입별 개별 점수 컬럼 생성
        scored_df = df
        
        # content_click 점수 (1점)
        scored_df = scored_df.withColumn(
            "content_click_score",
            when(col("eventType") == "content_click", 1).otherwise(0)
        )
        
        # like_click 점수 (3점)
        scored_df = scored_df.withColumn(
            "like_click_score",
            when(col("eventType") == "like_click", 3).otherwise(0)
        )
        
        # review_write 점수 (1점)
        scored_df = scored_df.withColumn(
            "review_write_score",
            when(col("eventType") == "review_write", 1).otherwise(0)
        )
        
        # rating_submit 점수 (1점)
        scored_df = scored_df.withColumn(
            "rating_submit_score",
            when(col("eventType") == "rating_submit", 1).otherwise(0)
        )
        
        # 점수별 통계 출력 (이벤트 타입별 분포 확인)
        logger.info("📈 이벤트별 점수 분포:")
        content_clicks = scored_df.filter(col("content_click_score") > 0).count()
        like_clicks = scored_df.filter(col("like_click_score") > 0).count()
        reviews = scored_df.filter(col("review_write_score") > 0).count()
        ratings = scored_df.filter(col("rating_submit_score") > 0).count()
        
        logger.info(f"  Content Click: {content_clicks:,}개 (총 {content_clicks}점)")
        logger.info(f"  Like Click: {like_clicks:,}개 (총 {like_clicks * 3}점)")
        logger.info(f"  Review Write: {reviews:,}개 (총 {reviews}점)")
        logger.info(f"  Rating Submit: {ratings:,}개 (총 {ratings}점)")
        
        self._log_step_time("점수 적용")
        return scored_df
    
    def aggregate_scores(self, df: DataFrame) -> DataFrame:
        """사용자별, 영화별 점수 집계 - 각 이벤트별 점수와 total_score 모두 집계"""
        logger.info("📈 점수 집계 시작...")
        
        # 사용자별, 비디오별 점수 집계 (각 이벤트별 점수를 합산하여 total_score 계산)
        aggregated_df = df.groupBy("userId", "videoId", "title") \
            .agg(
                # 각 이벤트별 점수 합계
                spark_sum("content_click_score").alias("content_click_total"),
                spark_sum("like_click_score").alias("like_click_total"),
                spark_sum("review_write_score").alias("review_write_total"),
                spark_sum("rating_submit_score").alias("rating_submit_total"),
                
                # 추가 메트릭
                count("*").alias("interaction_count"),
                current_timestamp().alias("processed_at")
            )
        
        # total_score 계산 (각 이벤트별 점수의 합)
        aggregated_df = aggregated_df.withColumn(
            "total_score",
            col("content_click_total") + 
            col("like_click_total") + 
            col("review_write_total") + 
            col("rating_submit_total")
        ).filter(col("total_score") > 0)  # 점수가 0보다 큰 것만
        
        # 집계 결과 통계
        total_interactions = aggregated_df.count()
        logger.info(f"✅ 집계 완료: {total_interactions:,}개 사용자-영화 상호작용")
        
        # 전체 점수 분포 확인
        score_dist = aggregated_df.groupBy("total_score").count().orderBy("total_score").collect()
        logger.info("📊 전체 점수 분포:")
        for row in score_dist[:10]:  # 상위 10개만 출력
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
        
        # 예시 데이터 출력 (LightFM 학습용 데이터 형태 확인)
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
        
        self._log_step_time("점수 집계")
        return aggregated_df
    
    def save_data(self, df: DataFrame) -> None:
        """결과를 MinIO에 저장"""
        logger.info("💾 데이터 저장 시작...")
        
        # 저장 경로 구성
        target_path = f"s3a://{self.minio_config.TARGET_BUCKET}/{self.process_date}/"
        
        logger.info(f"📁 저장 경로: {target_path}")
        
        try:
            # 날짜 컬럼 추가 (파티셔닝용)
            df_with_date = df.withColumn("process_date", lit(self.process_date))
            
            # Parquet으로 저장 (날짜별 파티셔닝)
            df_with_date.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .partitionBy("process_date") \
                .parquet(target_path)
            
            logger.info("✅ 데이터 저장 완료")
            
            # 저장된 파일 정보 확인
            saved_count = df.count()
            logger.info(f"📊 저장된 레코드 수: {saved_count:,}개")
            
            self._log_step_time("데이터 저장")
            
        except Exception as e:
            logger.error(f"❌ 데이터 저장 실패: {e}")
            raise
    
    def cleanup_spark(self) -> None:
        """Spark 세션 정리"""
        if self.spark:
            logger.info("🧹 Spark 세션 정리 중...")
            self.spark.stop()
            logger.info("✅ Spark 세션 정리 완료")
    
    def print_performance_summary(self) -> None:
        """처리 성능 요약 출력"""
        logger.info("\n" + "=" * 60)
        logger.info("📈 처리 성능 요약")
        logger.info("=" * 60)
        
        total_time = sum(self.step_times.values())
        logger.info(f"🕐 총 처리 시간: {total_time:.2f}초 ({total_time/60:.1f}분)")
        
        logger.info("\n단계별 처리 시간:")
        for step, elapsed in self.step_times.items():
            percentage = (elapsed / total_time) * 100
            logger.info(f"  {step}: {elapsed:.2f}초 ({percentage:.1f}%)")
        
        logger.info(f"\n🎯 처리 완료 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)
    
    def run(self) -> None:
        """전체 ETL 프로세스 실행"""
        logger.info("🎬 사용자 로그 집계 ELT 프로세스 시작")
        logger.info(f"📅 처리 대상 날짜: {self.process_date}")
        
        try:
            # 전체 시작 시간 기록
            overall_start = time.time()
            
            # 1. Spark 세션 생성
            self.create_spark_session()
            self._log_step_time("Spark 세션 생성")
            
            # 2. 데이터 로드
            raw_df = self.load_data()
            
            # 3. 점수 적용
            scored_df = self.apply_scoring(raw_df)
            
            # 4. 점수 집계
            aggregated_df = self.aggregate_scores(scored_df)
            
            # 5. 결과 저장
            self.save_data(aggregated_df)
            
            # 전체 처리 시간 계산
            total_elapsed = time.time() - overall_start
            self.step_times["전체 처리"] = total_elapsed
            
            # 성능 요약 출력
            self.print_performance_summary()
            
            logger.info("🎉 ELT 프로세스 성공적으로 완료!")
            
        except Exception as e:
            logger.error(f"❌ ELT 프로세스 실행 중 오류: {e}")
            raise
        finally:
            # 리소스 정리
            self.cleanup_spark()

def main():
    """메인 함수"""
    import argparse
    
    # 명령행 인자 파싱
    parser = argparse.ArgumentParser(description="사용자 로그 집계 ELT 처리")
    parser.add_argument(
        "--date", 
        type=str, 
        help="처리할 날짜 (YYYY-MM-DD), 기본값: 어제"
    )
    parser.add_argument(
        "--minio-endpoint",
        type=str,
        default="http://your-ec2-ip:9000",
        help="MinIO 엔드포인트"
    )
    
    args = parser.parse_args()
    
    # MinIO 엔드포인트 설정
    if args.minio_endpoint:
        MinIOConfig.ENDPOINT = args.minio_endpoint
    
    try:
        # ELT 프로세서 실행
        processor = UserLogELTProcessor(process_date=args.date)
        processor.run()
        
        # 성공 종료
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"❌ 프로그램 실행 실패: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

# =====================================================
# 🔧 실행 방법
# =====================================================
"""
1. 로컬 테스트:
   python userlog_agg_pyspark.py --date 2025-06-20

2. Spark Cluster 제출:
   spark-submit \
   --master spark://your-ec2-ip:7077 \
   --conf spark.pyspark.python=python3 \
   --conf spark.pyspark.driver.python=python3 \
   userlog_agg_pyspark.py --date 2025-06-20

3. 자동 실행 (어제 날짜):
   spark-submit --master spark://your-ec2-ip:7077 userlog_agg_pyspark.py

4. 의존성 패키지가 필요한 경우:
   spark-submit \
   --master spark://your-ec2-ip:7077 \
   --packages org.apache.hadoop:hadoop-aws:3.3.4 \
   userlog_agg_pyspark.py
"""

# =====================================================
# 📊 출력 데이터 스키마 (개선된 버전)
# =====================================================
"""
집계된 결과 데이터 스키마:
- userId: String (사용자 ID)
- videoId: String (비디오 ID) 
- title: String (영화 제목)
- content_click_total: Long (컨텐츠 클릭 총점)
- like_click_total: Long (좋아요 클릭 총점)
- review_write_total: Long (리뷰 작성 총점)
- rating_submit_total: Long (평점 제출 총점)
- total_score: Long (전체 상호작용 점수 = 위 4개 점수의 합)
- interaction_count: Long (상호작용 횟수)
- processed_at: Timestamp (처리 시각)
- process_date: String (처리 날짜, 파티션 키)

점수 체계:
- content_click: 1점
- like_click: 3점  
- review_write: 1점
- rating_submit: 1점

total_score 계산 공식:
total_score = content_click_total + like_click_total + review_write_total + rating_submit_total

저장 위치: s3a://ml-learning-data/YYYY-MM-DD/
파일 형식: Parquet (Snappy 압축)
파티셔닝: process_date별
"""
