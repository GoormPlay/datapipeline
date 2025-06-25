import json
import uuid
import random
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from pathlib import Path
import logging
from faker import Faker
import os
import psutil
import time

try:
    import dask
    import dask.dataframe as dd
    from dask.delayed import delayed
    from dask.distributed import Client, LocalCluster
    import dask.bag as db
    DASK_AVAILABLE = True
except ImportError:
    DASK_AVAILABLE = False

try:
    from minio import Minio
    from minio.error import S3Error
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False

try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# =====================================================
# 🔧 설정 - 여기서 변경하세요
# =====================================================

def get_optimal_chunk_size(memory_gb: int, total_logs: int) -> int:
    """메모리 크기에 따른 최적 청크 크기 계산"""
    if memory_gb >= 32:
        base_chunk = 50_000
    elif memory_gb >= 16:
        base_chunk = 20_000
    elif memory_gb >= 8:
        base_chunk = 10_000
    else:
        base_chunk = 5_000
    
    # 총 로그 수가 적으면 청크 크기 조정
    return min(base_chunk, max(1000, total_logs // 10))

# 기본 설정
LOG_COUNT = 7_500_000
USER_COUNT = 150_000
FILE_FORMAT = 'parquet'  # 'json' or 'parquet'
STORAGE_TYPE = 'local'   # 'local' or 'minio'
LOCAL_PATH = "/Users/eddie/Desktop/gp-datapipeline/datapipeline/utils/"

# 날짜 설정
DATE_CONFIG = {
    'start_date': '2025-06-23',
    'end_date': '2025-06-23',
    'date_range_days': None
}

# Dask 설정 (16GB 맥북 M3 Pro 최적화)
DASK_CONFIG = {
    'use_dask': True,
    'n_workers': 6,
    'threads_per_worker': 2,
    'memory_limit': '2GB',
    'chunk_size': get_optimal_chunk_size(16, LOG_COUNT),
    'dashboard_address': '8787',
    'dashboard_host': 'localhost'
}

# MinIO 설정
MINIO_CONFIG = {
    'endpoint': 'localhost:9000',
    'access_key': 'minioadmin',
    'secret_key': 'minioadmin',
    'bucket_name': 'userlog-data',
    'object_path': 'test/user-activity-raw/'
}

# 스큐 설정
SKEW_CONFIG = {
    'enable_skew': True,
    'heavy_user_ratio': 0.1,
    'heavy_user_activity_ratio': 0.8,
    'distribution_type': 'pareto'
}

# 이벤트 타입 분포
EVENT_WEIGHTS = {
    'content_click': 40,
    'play_start': 20,
    'like_click': 15,
    'play_stop': 10,
    'rating_submit': 8,
    'review_write': 5,
    'content_recom_click': 2
}

# 샘플 데이터
TITLES = [
    "미션 임파서블", "탑건: 매버릭", "어벤져스", "기생충", "오징어 게임",
    "킹덤", "스위트홈", "종이의 집", "스트레인저 띵스", "더 글로리"
]

VIDEO_IDS = [
    "vuOKO1LSIG8", "Tu2OkDzzMqY", "8sBVGbhVXY", "xK7S8cJkIkg",
    "dQw4w9WgXcQ", "jNQXAC9IVRw", "astISOttCQ0", "ZbZSe6N_BXs"
]

GENRES = ["액션", "드라마", "코미디", "로맨스", "스릴러", "호러", "SF", "판타지"]
PAGES = ["main", "content_detail", "content_play"]

# =====================================================
# 유틸리티 함수들
# =====================================================

def check_dependencies():
    """필수 의존성 검사"""
    missing_deps = []
    
    if not DASK_AVAILABLE:
        missing_deps.append("dask[complete]")
    
    if FILE_FORMAT == 'parquet' and not PARQUET_AVAILABLE:
        missing_deps.append("pandas pyarrow")
    
    if STORAGE_TYPE == 'minio' and not MINIO_AVAILABLE:
        missing_deps.append("minio")
    
    if missing_deps:
        logger.error(f"❌ 필수 라이브러리 누락: {', '.join(missing_deps)}")
        logger.error(f"   설치 명령: pip install {' '.join(missing_deps)}")
        return False
    
    return True

def validate_config():
    """설정 검증"""
    errors = []
    
    if LOG_COUNT <= 0:
        errors.append("LOG_COUNT는 양수여야 합니다")
    
    if USER_COUNT <= 0:
        errors.append("USER_COUNT는 양수여야 합니다")
    
    if FILE_FORMAT not in ['json', 'parquet']:
        errors.append("FILE_FORMAT은 'json' 또는 'parquet'이어야 합니다")
    
    if STORAGE_TYPE not in ['local', 'minio']:
        errors.append("STORAGE_TYPE은 'local' 또는 'minio'이어야 합니다")
    
    try:
        Path(LOCAL_PATH).resolve()
    except Exception:
        errors.append(f"LOCAL_PATH가 유효하지 않습니다: {LOCAL_PATH}")
    
    if errors:
        for error in errors:
            logger.error(f"❌ 설정 오류: {error}")
        return False
    
    return True

# =====================================================
# Dask 워커 함수들 (전역 스코프)
# =====================================================

def random_timestamp_worker() -> str:
    """워커에서 실행되는 타임스탬프 생성"""
    faker = Faker('ko_KR')
    
    if DATE_CONFIG.get('date_range_days'):
        end_date = datetime.now()
        start_date = end_date - timedelta(days=DATE_CONFIG['date_range_days'])
    else:
        try:
            start_date = datetime.strptime(DATE_CONFIG['start_date'], '%Y-%m-%d')
            end_date = datetime.strptime(DATE_CONFIG['end_date'], '%Y-%m-%d')
        except ValueError as e:
            logger.warning(f"날짜 파싱 오류: {e}, 기본값 사용")
            start_date = datetime.now() - timedelta(days=1)
            end_date = datetime.now()
    
    random_date = faker.date_time_between(start_date=start_date, end_date=end_date)
    return random_date.strftime('%Y-%m-%d %H:%M:%S')

def create_single_log_worker(user_id: str, seed: Optional[int] = None) -> Dict[str, Any]:
    """워커에서 실행되는 단일 로그 생성"""
    if seed:
        random.seed(seed)
    
    event_type = random.choices(
        list(EVENT_WEIGHTS.keys()),
        weights=list(EVENT_WEIGHTS.values()),
        k=1
    )[0]
    
    log = {
        'userId': user_id,
        'title': random.choice(TITLES),
        'videoId': random.choice(VIDEO_IDS),
        'timestamp': random_timestamp_worker(),
        'eventType': event_type,
        'genre': random.sample(GENRES, random.randint(1, 2)),
        'page': random.choice(PAGES)
    }
    
    # 이벤트별 추가 필드
    if event_type == 'rating_submit':
        log['rating'] = random.randint(1, 5)
    elif event_type == 'review_write':
        log['review'] = random.choice([
            "재미있어요!", "별로예요", "추천합니다", "시간 아까워요", "최고!"
        ])
    elif event_type == 'like_click':
        log['liked'] = random.choice([True, False])
    elif event_type == 'content_recom_click':
        log['recMovieList'] = random.sample(VIDEO_IDS, 4)
    
    return log

def create_log_batch_worker(user_logs_batch: List[tuple], base_seed: int = 42) -> List[Dict[str, Any]]:
    """워커에서 실행되는 배치 로그 생성"""
    logs = []
    
    for i, (user_id, log_count) in enumerate(user_logs_batch):
        seed = base_seed + i  # 각 사용자마다 다른 시드
        for j in range(log_count):
            log = create_single_log_worker(user_id, seed + j)
            logs.append(log)
    
    return logs

# =====================================================
# 핵심 클래스들
# =====================================================

class DaskClusterManager:
    """Dask 클러스터 관리"""
    
    def __init__(self):
        self.client: Optional[Client] = None
        self.cluster = None
    
    def setup(self) -> bool:
        """Dask 클러스터 설정"""
        if not DASK_AVAILABLE or not DASK_CONFIG['use_dask']:
            logger.info("📊 순차 처리 모드로 실행")
            return False
        
        try:
            # 시스템 리소스 확인
            cpu_count = os.cpu_count()
            memory_gb = psutil.virtual_memory().total / (1024**3)
            
            logger.info(f"🖥️  시스템: CPU {cpu_count}코어, 메모리 {memory_gb:.1f}GB")
            
            # 안전한 설정 계산
            n_workers = min(DASK_CONFIG['n_workers'], cpu_count - 2)
            memory_per_worker = min(
                int(memory_gb * 0.75 / n_workers),
                int(DASK_CONFIG['memory_limit'].rstrip('GB'))
            )
            
            logger.info(f"🔧 Dask: {n_workers}개 워커, 워커당 {memory_per_worker}GB")
            
            # 클러스터 생성
            from dask.distributed import LocalCluster
            self.cluster = LocalCluster(
                n_workers=n_workers,
                threads_per_worker=DASK_CONFIG['threads_per_worker'],
                memory_limit=f'{memory_per_worker}GB',
                dashboard_address=DASK_CONFIG['dashboard_address'],
                silence_logs=logging.ERROR,
                processes=True
            )
            
            self.client = Client(self.cluster)
            
            # 클러스터 테스트
            time.sleep(2)
            test_result = self.client.submit(lambda: "cluster_test").result(timeout=10)
            
            dashboard_url = f"http://{DASK_CONFIG['dashboard_host']}:{DASK_CONFIG['dashboard_address']}"
            logger.info(f"🚀 Dask 클러스터 시작 완료!")
            logger.info(f"📊 대시보드: {dashboard_url}")
            logger.info(f"✅ 클러스터 테스트: {test_result}")
            
            return True
            
        except Exception as e:
            logger.warning(f"⚠️  Dask 클러스터 실패: {e}")
            logger.info("   순차 처리로 전환합니다")
            self.client = None
            return False
    
    def close(self):
        """클러스터 정리"""
        if self.client:
            try:
                self.client.close()
                logger.info("🔚 Dask 클러스터 종료")
            except Exception as e:
                logger.warning(f"클러스터 종료 중 오류: {e}")

class SkewDistribution:
    """스큐 분포 생성기"""
    
    def __init__(self, user_count: int, config: Dict[str, Any]):
        self.user_count = user_count
        self.config = config
    
    def generate_distribution(self, total_logs: int) -> Dict[str, int]:
        """사용자별 활동량 분포 생성"""
        if not self.config.get('enable_skew', False):
            return self._uniform_distribution(total_logs)
        
        dist_type = self.config.get('distribution_type', 'pareto')
        
        if dist_type == 'pareto':
            return self._pareto_distribution(total_logs)
        elif dist_type == 'zipf':
            return self._zipf_distribution(total_logs)
        elif dist_type == 'power_law':
            return self._power_law_distribution(total_logs)
        else:
            logger.warning(f"Unknown distribution: {dist_type}, using pareto")
            return self._pareto_distribution(total_logs)
    
    def _uniform_distribution(self, total_logs: int) -> Dict[str, int]:
        """균등 분포"""
        logs_per_user = total_logs // self.user_count
        remainder = total_logs % self.user_count
        
        distribution = {}
        for i in range(self.user_count):
            user_id = f"user_{i:06d}"
            distribution[user_id] = logs_per_user + (1 if i < remainder else 0)
        
        logger.info("📊 균등 분포 생성")
        return distribution
    
    def _pareto_distribution(self, total_logs: int) -> Dict[str, int]:
        """파레토 분포 (80-20 법칙)"""
        heavy_ratio = self.config.get('heavy_user_ratio', 0.1)
        activity_ratio = self.config.get('heavy_user_activity_ratio', 0.8)
        
        heavy_count = max(1, int(self.user_count * heavy_ratio))
        normal_count = self.user_count - heavy_count
        
        heavy_logs = int(total_logs * activity_ratio)
        normal_logs = total_logs - heavy_logs
        
        distribution = {}
        
        # 헤비 유저
        heavy_logs_per_user = heavy_logs // heavy_count
        for i in range(heavy_count):
            user_id = f"heavy_user_{i:06d}"
            extra = heavy_logs % heavy_count if i == 0 else 0
            distribution[user_id] = heavy_logs_per_user + extra
        
        # 일반 유저
        if normal_count > 0:
            weights = np.exp(-np.linspace(0, 3, normal_count))
            weights = weights / weights.sum()
            
            for i in range(normal_count):
                user_id = f"normal_user_{i:06d}"
                distribution[user_id] = max(1, int(normal_logs * weights[i]))
        
        # 총합 조정
        self._adjust_total(distribution, total_logs)
        self._log_stats(distribution, "파레토")
        
        return distribution
    
    def _zipf_distribution(self, total_logs: int) -> Dict[str, int]:
        """Zipf 분포"""
        ranks = np.arange(1, self.user_count + 1)
        probabilities = 1 / (ranks ** 1.2)
        probabilities = probabilities / probabilities.sum()
        
        distribution = {}
        for i, prob in enumerate(probabilities):
            user_id = f"zipf_user_{i:06d}"
            distribution[user_id] = max(1, int(total_logs * prob))
        
        self._adjust_total(distribution, total_logs)
        self._log_stats(distribution, "Zipf")
        return distribution
    
    def _power_law_distribution(self, total_logs: int) -> Dict[str, int]:
        """Power Law 분포"""
        x = np.arange(1, self.user_count + 1)
        probabilities = x ** (-2.5)
        probabilities = probabilities / probabilities.sum()
        
        distribution = {}
        for i, prob in enumerate(probabilities):
            user_id = f"powerlaw_user_{i:06d}"
            distribution[user_id] = max(1, int(total_logs * prob))
        
        self._adjust_total(distribution, total_logs)
        self._log_stats(distribution, "Power Law")
        return distribution
    
    def _adjust_total(self, distribution: Dict[str, int], target: int):
        """총 로그 수 조정"""
        current = sum(distribution.values())
        diff = target - current
        if diff != 0 and distribution:
            first_user = next(iter(distribution))
            distribution[first_user] += diff
    
    def _log_stats(self, distribution: Dict[str, int], dist_type: str):
        """분포 통계"""
        if not distribution:
            return
            
        values = sorted(distribution.values(), reverse=True)
        total = sum(values)
        avg = total / len(values)
        
        top_10_percent = max(1, len(values) // 10)
        top_10_activity = sum(values[:top_10_percent]) / total * 100
        
        logger.info(f"📈 {dist_type} 분포 완료")
        logger.info(f"   최대/최소: {values[0]:,}/{values[-1]:,}개")
        logger.info(f"   상위 10%가 {top_10_activity:.1f}% 담당")
        logger.info(f"   스큐 비율: {values[0]/avg:.1f}x")

class DataGenerator:
    """로그 데이터 생성기"""
    
    def __init__(self, user_distribution: Dict[str, int], dask_client: Optional[Client] = None):
        self.user_distribution = user_distribution
        self.dask_client = dask_client
        self.chunk_size = DASK_CONFIG['chunk_size']
    
    def generate_logs(self) -> List[Dict[str, Any]]:
        """로그 생성 (Dask 또는 순차)"""
        if self.dask_client and DASK_AVAILABLE:
            return self._generate_parallel()
        else:
            return self._generate_sequential()
    
    def _generate_parallel(self) -> List[Dict[str, Any]]:
        """Dask 병렬 생성"""
        start_time = time.time()
        total_logs = sum(self.user_distribution.values())
        
        logger.info(f"🚀 병렬 생성 시작: {total_logs:,}개 로그")
        
        # 청크 분할
        chunks = self._create_chunks()
        logger.info(f"📦 {len(chunks)}개 청크 생성")
        
        # Delayed 작업 생성
        tasks = []
        for i, chunk in enumerate(chunks):
            seed = random.randint(1, 1000000) + i
            task = delayed(create_log_batch_worker)(chunk, seed)
            tasks.append(task)
        
        # 병렬 실행
        logger.info("⚡ 병렬 처리 중...")
        results = dask.compute(*tasks)
        
        # 결과 병합
        all_logs = []
        for batch in results:
            all_logs.extend(batch)
        
        # 시간순 셔플
        random.shuffle(all_logs)
        
        duration = time.time() - start_time
        speed = len(all_logs) / duration
        
        logger.info(f"✅ 병렬 생성 완료: {len(all_logs):,}개")
        logger.info(f"⏱️  시간: {duration:.2f}초, 속도: {speed:,.0f} 로그/초")
        
        return all_logs
    
    def _generate_sequential(self) -> List[Dict[str, Any]]:
        """순차 생성"""
        start_time = time.time()
        total_logs = sum(self.user_distribution.values())
        
        logger.info(f"📊 순차 생성 시작: {total_logs:,}개 로그")
        
        logs = []
        processed = 0
        
        for user_id, count in self.user_distribution.items():
            for i in range(count):
                log = create_single_log_worker(user_id, processed + i)
                logs.append(log)
                processed += 1
                
                if processed % 10000 == 0:
                    progress = processed / total_logs * 100
                    logger.info(f"   진행: {processed:,}/{total_logs:,} ({progress:.1f}%)")
        
        random.shuffle(logs)
        
        duration = time.time() - start_time
        logger.info(f"✅ 순차 생성 완료: {len(logs):,}개, {duration:.2f}초")
        
        return logs
    
    def _create_chunks(self) -> List[List[tuple]]:
        """청크 생성"""
        chunks = []
        current_chunk = []
        current_size = 0
        
        for user_id, count in self.user_distribution.items():
            if current_size + count > self.chunk_size and current_chunk:
                chunks.append(current_chunk)
                current_chunk = []
                current_size = 0
            
            current_chunk.append((user_id, count))
            current_size += count
        
        if current_chunk:
            chunks.append(current_chunk)
        
        return chunks

class FileManager:
    """파일 저장 관리"""
    
    # 고정된 스키마 순서 정의 (핵심 해결책!)
    FIXED_COLUMN_ORDER = [
        'userId', 'title', 'videoId', 'timestamp', 'eventType', 
        'genre', 'page', 'rating', 'review', 'liked', 'recMovieList'
    ]
    
    @staticmethod
    def save_logs(logs: List[Dict[str, Any]], filepath: Path):
        """로그 파일 저장"""
        start_time = time.time()
        
        # 디렉토리 생성
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        if FILE_FORMAT == 'json':
            FileManager._save_json(logs, filepath)
        elif FILE_FORMAT == 'parquet':
            FileManager._save_parquet(logs, filepath)
        else:
            raise ValueError(f"Unsupported format: {FILE_FORMAT}")
        
        # 파일 정보
        size_mb = filepath.stat().st_size / (1024 * 1024)
        duration = time.time() - start_time
        
        logger.info(f"💾 저장 완료: {size_mb:.2f}MB, {duration:.2f}초")
    
    @staticmethod
    def _save_json(logs: List[Dict[str, Any]], filepath: Path):
        """JSON 저장"""
        logger.info(f"💾 JSON 저장: {filepath}")
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for log in logs:
                json.dump(log, f, ensure_ascii=False, separators=(',', ':'))
                f.write('\n')
    
    @staticmethod
    def _save_parquet(logs: List[Dict[str, Any]], filepath: Path):
        """Parquet 저장 - 스키마 일관성 보장"""
        if not PARQUET_AVAILABLE:
            raise ImportError("pandas, pyarrow가 필요합니다")
        
        logger.info(f"💾 Parquet 저장: {filepath}")
        
        # 1. 스키마 일관성을 위한 데이터 정규화
        normalized_logs = FileManager._normalize_log_schema(logs)
        
        # 2. 청크 단위 저장 (메모리 효율)
        chunk_size = 50_000
        if len(normalized_logs) > chunk_size:
            logger.info(f"청크 단위 저장: {chunk_size:,}개씩")
            FileManager._save_parquet_chunked(normalized_logs, filepath, chunk_size)
        else:
            # 한 번에 저장
            df = pd.DataFrame(normalized_logs)
            df = FileManager._standardize_dataframe(df)
            
            table = pa.Table.from_pandas(df)
            pq.write_table(table, filepath, compression='snappy')
    
    @staticmethod
    def _normalize_log_schema(logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """로그 스키마 정규화 - 모든 로그가 동일한 필드를 가지도록"""
        normalized = []
        
        for log in logs:
            # 고정된 스키마로 정규화
            normalized_log = {}
            
            for column in FileManager.FIXED_COLUMN_ORDER:
                if column == 'rating':
                    # rating은 rating_submit 이벤트에만 존재
                    normalized_log[column] = log.get(column) if log.get('eventType') == 'rating_submit' else None
                elif column == 'review':
                    # review는 review_write 이벤트에만 존재
                    normalized_log[column] = log.get(column) if log.get('eventType') == 'review_write' else None
                elif column == 'liked':
                    # liked는 like_click 이벤트에만 존재
                    normalized_log[column] = log.get(column) if log.get('eventType') == 'like_click' else None
                elif column == 'recMovieList':
                    # recMovieList는 content_recom_click 이벤트에만 존재
                    normalized_log[column] = log.get(column) if log.get('eventType') == 'content_recom_click' else None
                else:
                    # 기본 필드들
                    normalized_log[column] = log.get(column)
            
            normalized.append(normalized_log)
        
        return normalized
    
    @staticmethod
    def _save_parquet_chunked(logs: List[Dict[str, Any]], filepath: Path, chunk_size: int):
        """청크 단위 Parquet 저장 - 스키마 일관성 보장"""
        
        # 첫 번째 청크로 마스터 스키마 생성
        first_chunk = logs[:chunk_size]
        first_df = pd.DataFrame(first_chunk)
        first_df = FileManager._standardize_dataframe(first_df)
        
        # 마스터 스키마 생성
        master_schema = pa.Table.from_pandas(first_df).schema
        logger.info(f"📋 마스터 스키마 생성: {len(master_schema)} 컬럼")
        
        # Parquet Writer 초기화
        writer = pq.ParquetWriter(filepath, master_schema, compression='snappy')
        
        try:
            # 첫 번째 청크 저장
            table = pa.Table.from_pandas(first_df, schema=master_schema)
            writer.write_table(table)
            
            # 나머지 청크들 저장
            for i in range(chunk_size, len(logs), chunk_size):
                chunk = logs[i:i+chunk_size]
                df = pd.DataFrame(chunk)
                df = FileManager._standardize_dataframe(df)
                
                # 컬럼 순서를 마스터 스키마와 맞춤
                df = df.reindex(columns=[field.name for field in master_schema])
                
                # 스키마 강제 적용
                table = pa.Table.from_pandas(df, schema=master_schema)
                writer.write_table(table)
                
                if i % (chunk_size * 4) == 0:  # 진행률 표시
                    progress = min(100, (i / len(logs)) * 100)
                    logger.info(f"   저장 진행률: {progress:.1f}%")
        
        finally:
            writer.close()
    
    @staticmethod
    def _standardize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """DataFrame 표준화 - 데이터 타입 및 컬럼 순서 고정"""
        
        # 1. 컬럼 순서 고정
        df = df.reindex(columns=FileManager.FIXED_COLUMN_ORDER)
        
        # 2. 데이터 타입 최적화
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # nullable integer 사용 (pandas 1.0+)
        if 'rating' in df.columns:
            df['rating'] = df['rating'].astype('Int8')  # nullable integer
        
        # nullable boolean 사용
        if 'liked' in df.columns:
            df['liked'] = df['liked'].astype('boolean')  # nullable boolean
        
        # 3. 리스트 타입 필드들 처리
        for col in ['genre', 'recMovieList']:
            if col in df.columns:
                # None 값을 빈 리스트로 변환
                df[col] = df[col].apply(lambda x: x if x is not None else [])
        
        # 4. 문자열 필드들의 None 처리
        for col in ['review']:
            if col in df.columns:
                df[col] = df[col].astype('string')  # nullable string
        
        return df

class MinioUploader:
    """MinIO 업로더"""
    
    def __init__(self, config: Dict[str, str]):
        if not MINIO_AVAILABLE:
            raise ImportError("minio 라이브러리가 필요합니다")
        
        self.config = config
        self.client = Minio(
            config['endpoint'],
            access_key=config['access_key'],
            secret_key=config['secret_key'],
            secure=False
        )
    
    def upload(self, filepath: Path, filename: str) -> bool:
        """파일 업로드"""
        try:
            bucket = self.config['bucket_name']
            object_name = f"{self.config['object_path']}{filename}"
            
            logger.info(f"☁️  MinIO 업로드: {bucket}/{object_name}")
            
            if not self.client.bucket_exists(bucket):
                self.client.make_bucket(bucket)
                logger.info(f"버킷 생성: {bucket}")
            
            self.client.fput_object(bucket, object_name, str(filepath))
            logger.info("✅ 업로드 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ 업로드 실패: {e}")
            return False

def analyze_data(logs: List[Dict[str, Any]]):
    """데이터 분석"""
    if not logs:
        logger.warning("분석할 데이터가 없습니다")
        return
    
    user_counts = {}
    event_counts = {}
    
    for log in logs:
        user_id = log.get('userId', 'unknown')
        event_type = log.get('eventType', 'unknown')
        
        user_counts[user_id] = user_counts.get(user_id, 0) + 1
        event_counts[event_type] = event_counts.get(event_type, 0) + 1
    
    # 사용자 분석
    activity_values = sorted(user_counts.values(), reverse=True)
    total_logs = len(logs)
    total_users = len(activity_values)
    avg_activity = total_logs / total_users if total_users > 0 else 0
    
    top_10_percent = max(1, total_users // 10)
    top_10_activity = sum(activity_values[:top_10_percent]) / total_logs * 100 if total_logs > 0 else 0
    
    logger.info(f"\n📊 데이터 분석 결과:")
    logger.info(f"   전체 사용자: {total_users:,}명")
    logger.info(f"   전체 로그: {total_logs:,}개")
    logger.info(f"   최고 활성 사용자: {activity_values[0]:,}개 로그")
    logger.info(f"   평균 활동: {avg_activity:.1f}개 로그")
    logger.info(f"   상위 10% 사용자가 {top_10_activity:.1f}% 활동")
    
    if avg_activity > 0:
        logger.info(f"   스큐 비율: {activity_values[0]/avg_activity:.1f}x")
    
    # 이벤트 분포
    logger.info(f"\n📈 이벤트 타입 분포:")
    for event_type, count in sorted(event_counts.items(), key=lambda x: x[1], reverse=True):
        percentage = count / total_logs * 100 if total_logs > 0 else 0
        logger.info(f"   {event_type}: {count:,}개 ({percentage:.1f}%)")

def main():
    """메인 실행 함수"""
    logger.info("=" * 60)
    logger.info("🚀 Dask 최적화 스큐 데이터 생성기")
    logger.info(f"📊 로그 수: {LOG_COUNT:,}개")
    logger.info(f"👥 사용자 수: {USER_COUNT:,}명")
    logger.info(f"🔥 스큐 활성화: {SKEW_CONFIG['enable_skew']}")
    logger.info(f"⚡ Dask 활성화: {DASK_CONFIG['use_dask']}")
    logger.info(f"📦 청크 크기: {DASK_CONFIG['chunk_size']:,}개")
    logger.info(f"💾 저장 형식: {FILE_FORMAT}")
    logger.info(f"📁 저장 경로: {LOCAL_PATH}")
    
    if DATE_CONFIG.get('date_range_days'):
        logger.info(f"📅 날짜 범위: 최근 {DATE_CONFIG['date_range_days']}일")
    else:
        logger.info(f"📅 날짜 범위: {DATE_CONFIG['start_date']} ~ {DATE_CONFIG['end_date']}")
    
    logger.info("=" * 60)
    
    # 사전 검증
    if not check_dependencies():
        return 1
    
    if not validate_config():
        return 1
    
    # Dask 클러스터 시작
    cluster_manager = DaskClusterManager()
    
    try:
        # 1. Dask 클러스터 설정
        logger.info("1️⃣  Dask 클러스터 설정 중...")
        dask_available = cluster_manager.setup()
        
        # 2. 사용자 분포 생성
        logger.info("2️⃣  사용자 분포 생성 중...")
        skew_gen = SkewDistribution(USER_COUNT, SKEW_CONFIG)
        user_distribution = skew_gen.generate_distribution(LOG_COUNT)
        
        # 3. 로그 데이터 생성
        logger.info("3️⃣  로그 데이터 생성 중...")
        data_gen = DataGenerator(user_distribution, cluster_manager.client)
        logs = data_gen.generate_logs()
        
        # 4. 파일 저장
        logger.info("4️⃣  파일 저장 중...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        skew_suffix = "_skewed" if SKEW_CONFIG['enable_skew'] else "_uniform"
        dask_suffix = "_dask" if dask_available else "_sequential"
        filename = f"user_activity_logs{skew_suffix}{dask_suffix}_{timestamp}.{FILE_FORMAT}"
        filepath = Path(LOCAL_PATH) / filename
        
        FileManager.save_logs(logs, filepath)
        
        # 5. MinIO 업로드 (선택사항)
        if STORAGE_TYPE == 'minio':
            logger.info("5️⃣  MinIO 업로드 중...")
            try:
                uploader = MinioUploader(MINIO_CONFIG)
                success = uploader.upload(filepath, filename)
                if not success:
                    logger.warning("⚠️  MinIO 업로드 실패, 로컬 파일 사용")
            except Exception as e:
                logger.error(f"MinIO 업로드 오류: {e}")
        
        # 6. 데이터 분석
        logger.info("6️⃣  데이터 분석 중...")
        analyze_data(logs)
        
        # 완료 메시지
        logger.info(f"\n✅ 모든 작업 완료!")
        logger.info(f"📁 파일 위치: {filepath}")
        
        if STORAGE_TYPE == 'minio':
            logger.info(f"🌍 MinIO: {MINIO_CONFIG['bucket_name']}/{MINIO_CONFIG['object_path']}{filename}")
        
        if dask_available:
            dashboard_url = f"http://{DASK_CONFIG['dashboard_host']}:{DASK_CONFIG['dashboard_address']}"
            logger.info(f"📊 Dask 대시보드: {dashboard_url}")
        
        logger.info("💡 Spark에서 이 데이터로 스큐 테스트를 진행하세요.")
        
        return 0
        
    except KeyboardInterrupt:
        logger.info("\n🛑 사용자에 의해 중단됨")
        return 1
    except Exception as e:
        logger.error(f"❌ 실행 중 오류: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        cluster_manager.close()

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)