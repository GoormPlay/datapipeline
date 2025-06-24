#!/usr/bin/env python3
# kafka_consumer_benchmark.py

import argparse
import time
import json
import csv
import threading
import psutil
import statistics
from datetime import datetime
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

# 기본 설정
KAFKA_BOOTSTRAP_SERVERS = '15.165.7.37:9092,3.35.6.38:9092,3.38.200.113:9092'
SCHEMA_REGISTRY_URL = 'http://15.165.7.37:8081'
KAFKA_TOPIC = 'userlog-avro-benchmark'
GROUP_ID_PREFIX = 'consumer-benchmark'

# 성능 측정을 위한 클래스
class PerformanceMonitor:
    def __init__(self, scenario_name, config_name):
        self.scenario_name = scenario_name
        self.config_name = config_name
        self.start_time = None
        self.end_time = None
        self.message_count = 0
        self.processing_times = []
        self.cpu_samples = []
        self.memory_samples = []
        self.lag_samples = []  # 컨슈머 지연(lag) 측정
        self.monitoring = False
        self.monitor_thread = None

    def start(self):
        self.start_time = time.time()
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_resources)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        print(f"🔍 성능 모니터링 시작: {self.scenario_name} - {self.config_name}")

    def stop(self):
        self.end_time = time.time()
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=1.0)
        print(f"🛑 성능 모니터링 종료: {self.scenario_name} - {self.config_name}")

    def record_message_processed(self, processing_time_ms):
        self.message_count += 1
        self.processing_times.append(processing_time_ms)

    def record_lag(self, lag):
        self.lag_samples.append(lag)

    def _monitor_resources(self):
        while self.monitoring:
            # CPU와 메모리 사용량 측정
            process = psutil.Process()
            self.cpu_samples.append(process.cpu_percent(interval=0.1))
            self.memory_samples.append(process.memory_info().rss / 1024 / 1024)  # MB 단위
            time.sleep(1.0)  # 1초마다 측정

    def get_results(self):
        if self.start_time is None or self.end_time is None:
            return None

        duration = self.end_time - self.start_time
        msgs_per_sec = self.message_count / duration if duration > 0 else 0

        # 통계 계산
        avg_processing_time = statistics.mean(self.processing_times) if self.processing_times else 0
        p95_processing_time = statistics.quantiles(self.processing_times, n=20)[18] if len(self.processing_times) > 20 else 0
        p99_processing_time = statistics.quantiles(self.processing_times, n=100)[98] if len(self.processing_times) > 100 else 0
        
        avg_cpu = statistics.mean(self.cpu_samples) if self.cpu_samples else 0
        avg_memory = statistics.mean(self.memory_samples) if self.memory_samples else 0
        
        avg_lag = statistics.mean(self.lag_samples) if self.lag_samples else 0
        max_lag = max(self.lag_samples) if self.lag_samples else 0

        return {
            'scenario': self.scenario_name,
            'config': self.config_name,
            'total_messages': self.message_count,
            'duration_sec': round(duration, 2),
            'throughput_msgs_sec': round(msgs_per_sec, 2),
            'avg_processing_time_ms': round(avg_processing_time, 2),
            'p95_processing_time_ms': round(p95_processing_time, 2),
            'p99_processing_time_ms': round(p99_processing_time, 2),
            'avg_cpu_percent': round(avg_cpu, 2),
            'avg_memory_mb': round(avg_memory, 2),
            'avg_lag': round(avg_lag, 2),
            'max_lag': round(max_lag, 2)
        }


# 메시지 처리 함수 - 실제 분석/처리 작업을 시뮬레이션
def process_message(message):
    # 메시지 처리를 시뮬레이션하는 간단한 지연
    # 실제 환경에서는 여기서 데이터 분석, DB 저장 등의 작업이 이루어집니다
    time.sleep(0.001)  # 1ms의 처리 시간 시뮬레이션


# 단일 컨슈머 테스트
# 단일 컨슈머 테스트
def run_single_consumer_test(scenario, config, max_messages, timeout=300):
    consumer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': f"{GROUP_ID_PREFIX}-{scenario['name']}-{config['name']}-{int(time.time())}",  # 유니크한 그룹 ID
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'fetch.min.bytes': config['fetch_min_bytes'],
        'max.partition.fetch.bytes': config['max_partition_fetch_bytes'],
    }
    
    print(f"\n{'=' * 60}")
    print(f"🧪 단일 컨슈머 테스트 시작: {scenario['name']} - {config['name']}")
    print(f"⚙️ 설정: fetch.min.bytes={config['fetch_min_bytes']}, max.partition.fetch.bytes={config['max_partition_fetch_bytes']}")
    print(f"{'=' * 60}")
    
    monitor = PerformanceMonitor(scenario['name'], config['name'])
    
    # Schema Registry 설정
    schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    avro_deserializer = AvroDeserializer(schema_registry_client)
    
    try:
        consumer = Consumer(consumer_config)
        consumer.subscribe([KAFKA_TOPIC])
        
        # 성능 모니터링 시작
        monitor.start()
        
        message_count = 0
        start_time = time.time()
        last_progress_time = start_time
        
        while message_count < max_messages:
            # 타임아웃 체크
            if time.time() - start_time > timeout:
                print(f"⚠️ 시간 제한 초과: {timeout}초")
                break
            
            # 메시지 가져오기
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"❓ 파티션 끝에 도달했습니다: {msg.topic()}, 파티션: {msg.partition()}")
                    continue
                else:
                    print(f"❌ 오류: {msg.error()}")
                    break
            
            # 메시지 처리 시작 시간
            msg_process_start = time.time()
            
            try:
                # Avro 메시지 디시리얼라이즈
                value = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                
                # 메시지 처리
                process_message(value)
                
                # 처리 시간 측정 (밀리초)
                processing_time_ms = (time.time() - msg_process_start) * 1000
                
                # 성능 모니터에 기록
                monitor.record_message_processed(processing_time_ms)
                
                # 메시지의 타임스탬프와 현재 시간을 비교하여 지연 측정 (밀리초)
                event_time = value.get('timestamp', time.time() * 1000)  # 밀리초 단위 타임스탬프
                current_time = int(time.time() * 1000)
                lag_ms = current_time - event_time
                monitor.record_lag(lag_ms)
                
                message_count += 1
            
            except Exception as e:
                print(f"❌ 메시지 처리 오류: {e}")
            
            # 진행 상황 출력
            current_time = time.time()
            if current_time - last_progress_time >= 5 or message_count >= max_messages:
                elapsed = current_time - start_time
                msgs_per_sec = message_count / elapsed if elapsed > 0 else 0
                print(f"🚀 진행 중: {message_count}/{max_messages} 메시지 처리됨 ({msgs_per_sec:.1f} msgs/sec)")
                last_progress_time = current_time
        
    except KafkaException as e:
        print(f"❌ Kafka 오류: {e}")
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {e}")
    finally:
        try:
            # 성능 모니터링 종료
            monitor.stop()
            
            # 컨슈머 정리
            consumer.close()
            
            print(f"✅ 테스트 완료: {scenario['name']} - {config['name']}, {monitor.message_count} 메시지 처리됨")
            
            return monitor.get_results()
        except:
            pass


# 멀티스레드 컨슈머 테스트
def run_multithreaded_consumer_test(scenario, config, max_messages, num_threads=4, timeout=300):
    base_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': f"{GROUP_ID_PREFIX}-mt-{scenario['name']}-{config['name']}",
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'fetch.min.bytes': config['fetch_min_bytes'],
        'max.partition.fetch.bytes': config['max_partition_fetch_bytes'],
        'schema.registry.url': SCHEMA_REGISTRY_URL,
    }
    
    print(f"\n{'=' * 60}")
    print(f"🧪 멀티스레드 컨슈머 테스트 시작: {scenario['name']} - {config['name']} (스레드 수: {num_threads})")
    print(f"⚙️ 설정: fetch.min.bytes={config['fetch_min_bytes']}, max.partition.fetch.bytes={config['max_partition_fetch_bytes']}")
    print(f"{'=' * 60}")
    
    # 공유 변수 설정
    message_count = 0
    monitor = PerformanceMonitor(f"{scenario['name']}-MT{num_threads}", config['name'])
    lock = threading.Lock()
    should_stop = threading.Event()
    
    def worker_thread(thread_id):
        nonlocal message_count
        
        # 각 스레드의 컨슈머는 같은 그룹 ID를 사용하여 파티션이 자동 분배됨
        consumer_config = base_config.copy()
        consumer_config['client.id'] = f"thread-{thread_id}"
        
        try:
            consumer = Consumer(consumer_config)
            consumer.subscribe([KAFKA_TOPIC])
            
            local_count = 0
            
            while not should_stop.is_set():
                msg = consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"❌ 스레드 {thread_id} 오류: {msg.error()}")
                        break
                
                # 메시지 처리 시작 시간
                msg_process_start = time.time()
                
                # 메시지 처리
                process_message(msg.value())
                
                # 처리 시간 측정 (밀리초)
                processing_time_ms = (time.time() - msg_process_start) * 1000
                
                with lock:
                    # 처리된 메시지 기록
                    monitor.record_message_processed(processing_time_ms)
                    # 메시지 카운트 증가
                    message_count += 1
                    local_count += 1
                    
                    # 메시지의 타임스탬프와 현재 시간 비교하여 지연 측정
                    event_time = msg.value().get('timestamp', time.time() * 1000)
                    current_time = int(time.time() * 1000)
                    lag_ms = current_time - event_time
                    monitor.record_lag(lag_ms)
                    
                    if message_count >= max_messages:
                        should_stop.set()
                
                # 주기적으로 진행 상황 기록 (스레드 별)
                if local_count % 5000 == 0:
                    print(f"📌 스레드 {thread_id}: {local_count} 메시지 처리 완료")
        
        except Exception as e:
            print(f"❌ 스레드 {thread_id} 예외: {e}")
        finally:
            try:
                consumer.close()
                print(f"🧵 스레드 {thread_id} 종료 (처리된 메시지 수: {local_count})")
            except:
                pass
    
    try:
        # 모니터링 시작
        monitor.start()
        
        # 스레드 생성 및 시작
        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=worker_thread, args=(i,))
            t.daemon = True
            t.start()
            threads.append(t)
        
        # 타임아웃 또는 메시지 수 초과 확인
        start_time = time.time()
        last_progress_time = start_time
        
        while not should_stop.is_set() and time.time() - start_time < timeout:
            time.sleep(1.0)
            
            current_time = time.time()
            if current_time - last_progress_time >= 5:
                elapsed = current_time - start_time
                with lock:
                    msgs_per_sec = message_count / elapsed if elapsed > 0 else 0
                    print(f"🚀 진행 중: {message_count}/{max_messages} 메시지 처리됨 ({msgs_per_sec:.1f} msgs/sec)")
                last_progress_time = current_time
            
            with lock:
                if message_count >= max_messages:
                    should_stop.set()
        
        # 타임아웃 처리
        if time.time() - start_time >= timeout:
            print(f"⚠️ 시간 제한 초과: {timeout}초")
            should_stop.set()
        
        # 모든 스레드 종료 대기
        for t in threads:
            t.join(timeout=5.0)
        
    except Exception as e:
        print(f"❌ 멀티스레드 테스트 오류: {e}")
    finally:
        # 모니터링 종료
        monitor.stop()
        
        print(f"✅ 멀티스레드 테스트 완료: {scenario['name']} - {config['name']}, {message_count} 메시지 처리됨")
        
        return monitor.get_results()


def run_scenario_tests(scenario, configs, parallel_strategies=None):
    """시나리오별 테스트 실행"""
    print(f"\n🌟 시나리오 '{scenario['name']}' 테스트 시작")
    
    max_messages = scenario['messages']
    results = []
    
    # 단일 컨슈머 테스트
    for config in configs:
        result = run_single_consumer_test(scenario, config, max_messages)
        if result:
            results.append(result)
        time.sleep(2)  # 테스트 간 간격 유지
    
    # 병렬 처리 테스트 (요청한 경우)
    if parallel_strategies:
        best_config = max(configs, key=lambda c: c.get('priority', 0))
        print(f"\n🔍 병렬 처리 테스트에 최적 설정 '{best_config['name']}' 사용")
        
        for strategy in parallel_strategies:
            if strategy['type'] == 'multithreaded':
                for thread_count in strategy['thread_counts']:
                    result = run_multithreaded_consumer_test(
                        scenario, best_config, max_messages, num_threads=thread_count
                    )
                    if result:
                        results.append(result)
                    time.sleep(2)  # 테스트 간 간격 유지
    
    return results


def main():
    parser = argparse.ArgumentParser(description='Kafka 컨슈머 성능 벤치마크')
    parser.add_argument('--scenario', type=str, choices=['normal', 'weekend', 'special', 'all'], default='normal',
                        help='테스트할 시나리오 (normal, weekend, special, all)')
    parser.add_argument('--parallel', action='store_true',
                        help='병렬 처리 테스트 수행 여부')
    args = parser.parse_args()
    
    # 테스트할 시나리오 정의
    scenarios = {
        'normal': {
            'name': '일반 평일',
            'messages': 300000,  # 테스트용 메시지 수 (실제 환경에 맞게 조정)
        },
        'weekend': {
            'name': '주말 피크',
            'messages': 400000,  # 테스트용 메시지 수 (실제 환경에 맞게 조정)
        },
        'special': {
            'name': '특별 이벤트',
            'messages': 500000,  # 테스트용 메시지 수 (실제 환경에 맞게 조정)
        }
    }
    
    # 테스트할 컨슈머 설정 정의
    configs = [
        {
            'name': '기본 설정',
            'fetch_min_bytes': 1,
            'max_poll_records': 500,
            'max_partition_fetch_bytes': 1048576,  # 1MB
            'priority': 1
        },
        {
            'name': '중간 배치',
            'fetch_min_bytes': 4096,
            'max_poll_records': 1000,
            'max_partition_fetch_bytes': 4194304,  # 4MB
            'priority': 2
        },
        {
            'name': '대용량 배치',
            'fetch_min_bytes': 16384,
            'max_poll_records': 2500,
            'max_partition_fetch_bytes': 10485760,  # 10MB
            'priority': 3
        },
        {
            'name': '초대용량 배치',
            'fetch_min_bytes': 65536,
            'max_poll_records': 5000,
            'max_partition_fetch_bytes': 10485760,  # 10MB
            'priority': 4
        }
    ]
    
    # 병렬 처리 전략
    parallel_strategies = [
        {
            'type': 'multithreaded',
            'thread_counts': [3, 6, 12]
        }
    ] if args.parallel else None
    
    all_results = []
    
    # 선택된 시나리오에 따라 테스트 실행
    if args.scenario == 'all':
        for scenario_id, scenario in scenarios.items():
            results = run_scenario_tests(scenario, configs, parallel_strategies)
            all_results.extend(results)
    else:
        scenario = scenarios[args.scenario]
        results = run_scenario_tests(scenario, configs, parallel_strategies)
        all_results.extend(results)
    
    # 결과 CSV 저장
    if all_results:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"kafka_consumer_benchmark_{timestamp}.csv"
        
        with open(filename, 'w', newline='') as csvfile:
            fieldnames = all_results[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for result in all_results:
                writer.writerow(result)
        
        print(f"\n✅ 결과가 '{filename}' 파일에 저장되었습니다.")
        
        # 결과 요약
        print("\n📊 테스트 결과 요약:")
        
        # 처리량 기준 상위 결과
        print("\n🏆 처리량 기준 상위 3개 설정:")
        top_throughput = sorted(all_results, key=lambda x: x['throughput_msgs_sec'], reverse=True)[:3]
        for i, result in enumerate(top_throughput):
            print(f"{i+1}위: {result['scenario']} - {result['config']}, " 
                  f"처리량: {result['throughput_msgs_sec']} msgs/sec, "
                  f"지연: {result['avg_lag']}ms")
        
        # 지연 시간 기준 상위 결과
        print("\n⏱️ 지연 시간 기준 상위 3개 설정 (낮을수록 좋음):")
        top_latency = sorted(all_results, key=lambda x: x['avg_processing_time_ms'])[:3]
        for i, result in enumerate(top_latency):
            print(f"{i+1}위: {result['scenario']} - {result['config']}, "
                  f"평균 처리 시간: {result['avg_processing_time_ms']}ms, "
                  f"처리량: {result['throughput_msgs_sec']} msgs/sec")
        
        # 리소스 효율성 기준 상위 결과
        print("\n💻 리소스 효율성 기준 상위 3개 설정 (처리량/CPU 비율이 높을수록 좋음):")
        resource_efficiency = [(r, r['throughput_msgs_sec'] / r['avg_cpu_percent'] if r['avg_cpu_percent'] > 0 else 0) 
                               for r in all_results]
        top_efficiency = sorted(resource_efficiency, key=lambda x: x[1], reverse=True)[:3]
        for i, (result, efficiency) in enumerate(top_efficiency):
            print(f"{i+1}위: {result['scenario']} - {result['config']}, "
                  f"효율성 점수: {efficiency:.2f}, "
                  f"CPU: {result['avg_cpu_percent']}%, "
                  f"메모리: {result['avg_memory_mb']}MB")


if __name__ == "__main__":
    main()

# 테스트 명령어
# 일반 평일 시나리오만 테스트
# python kafka_consumer_benchmark.py --scenario normal

# # 주말 피크 시나리오 + 병렬 처리 테스트
# python kafka_consumer_benchmark.py --scenario weekend --parallel

# # 특별 이벤트 시나리오 테스트
# python kafka_consumer_benchmark.py --scenario special

# # 모든 시나리오 테스트
# python kafka_consumer_benchmark.py --scenario all