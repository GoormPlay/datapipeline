#!/usr/bin/env python3
# kafka_transaction_benchmark.py

import argparse
import random
import time
from datetime import datetime
import json
import csv
from faker import Faker

# confluent_kafka 및 Avro 관련 import
from confluent_kafka.avro import AvroProducer
from confluent_kafka import KafkaError as ConfluentKafkaError

# MongoDB 연결을 위한 pymongo import
try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure
    PYMONGO_AVAILABLE = True
except ImportError:
    PYMONGO_AVAILABLE = False
    MongoClient = None
    ConnectionFailure = None

# 기본 설정
kafka_cluster = '15.165.7.37:9092,3.35.6.38:9092,3.38.200.113:9092'
SCHEMA_REGISTRY_URL = 'http://15.165.7.37:8081'
KAFKA_TOPIC_AVRO = 'userlog-avro-benchmark' # 벤치마크용 토픽
# Avro 스키마 정의
AVRO_SCHEMA_STRING = """
{
  "type": "record",
  "name": "UserEvent",
  "fields": [
    {"name": "videoId", "type": ["null", "string"]},
    {"name": "title", "type": ["null", "string"]},
    {"name": "userId", "type": ["null","string"]},
    {"name": "timestamp", "type": ["null", "long"]},
    {"name": "eventType", "type": ["null", "string"]},
    {"name": "page", "type": ["null", "string"]},
    {"name": "liked", "type": ["null", "boolean"]},
    {"name": "review", "type": ["null", "string"]},
    {"name": "rating", "type": ["null", "int"]},
    {"name": "genre", "type": ["null", {"type": "array", "items": "string"}], "default": null},
    {"name": "recMovieList", "type": ["null", "string"], "default": null}
  ]
}
"""

# 성능 데이터를 저장할 리스트
performance_data = []

def delivery_report(err, msg):
    """메시지 전송 결과를 보고하는 콜백 함수"""
    if err is not None:
        print(f"❌ 메시지 전송 실패: {err}")
    else:
        # 성공한 메시지의 0.01%만 로깅하여 출력량 제한
        if random.random() < 0.0001:
            print(f"✅ 메시지 전송 성공: {msg.topic()} [파티션 {msg.partition()}] @ 오프셋 {msg.offset()}")

def fetch_mongo_data():
    """MongoDB에서 콘텐츠 데이터를 가져오는 함수"""
    mongo_contents_data = []
    if PYMONGO_AVAILABLE:
        try:
            client = MongoClient('mongodb+srv://user:goorm0508@goorm-mongodb.svz66jf.mongodb.net/?retryWrites=true&w=majority&appName=goorm-mongoDB')
            client.admin.command('ping')
            db = client['content-db']
            contents_collection = db['contents']
            mongo_contents_data = list(contents_collection.find({}, {"_id": 0, "title": 1, "videoId": 1, "genre": 1}))
            client.close()
            if mongo_contents_data:
                print(f"✅ MongoDB에서 {len(mongo_contents_data)}개 항목을 성공적으로 가져왔습니다.")
            else:
                print("ℹ️ MongoDB에서 데이터를 가져오지 못했거나 컬렉션이 비어 있습니다.")
        except Exception as e:
            print(f"❌ MongoDB 데이터 가져오기 오류: {e}")
    return mongo_contents_data

def make_event_payload(fake, mongo_contents_data):
    """이벤트 페이로드를 생성하는 함수"""
    event_types = ["like_click", "content_click", "review_write", "rating_submit", "play_start", "play_stop", "content_recom_click"]
    
    event = {
        "videoId": None,
        "title": None,
        "userId": None,
        "timestamp": None,
        "eventType": None,
        "page": None,
        "liked": None,
        "review": None,
        "rating": None,
        "genre": None,
        "recMovieList": None
    }
    
    if mongo_contents_data:
        selected_content = random.choice(mongo_contents_data)
        event["videoId"] = selected_content.get("videoId")
        event["title"] = selected_content.get("title")
        event["genre"] = selected_content.get("genre")
    
    fake_datetime = fake.date_time_between(start_date="-1d", end_date="now")
    unix_timestamp = int(fake_datetime.timestamp() * 1000)
    
    event.update({
        "userId": fake.uuid4(),
        "timestamp": unix_timestamp,
        "eventType": random.choice(event_types)
    })
    
    if event["eventType"] == "like_click":
        event["page"] = "content_detail"
        event["liked"] = random.choice([True, False])
    elif event["eventType"] == "review_write":
        event["page"] = "content_detail"
        event["review"] = fake.sentence()
    elif event["eventType"] == "rating_submit":
        event["page"] = "content_detail"
        event["rating"] = random.randint(1, 5)
    elif event["eventType"] == "content_click":
        event["page"] = "content_detail"
    elif event["eventType"] == "content_recom_click":
        event["page"] = "content_detail"
        event["recMovieList"] = fake.sentence()
    elif event["eventType"] == "play_start":
        event["page"] = "content_play"
    elif event["eventType"] == "play_stop":
        event["page"] = "content_play"
    
    return event

def run_benchmark(transaction_size, compression_type, num_events, linger_ms=200):
    """지정된 매개변수로 벤치마크 테스트를 실행하는 함수"""
    fake = Faker()
    mongo_contents_data = fetch_mongo_data()
    
    # 프로듀서 설정
    producer_config = {
        'bootstrap.servers': kafka_cluster,
        'schema.registry.url': SCHEMA_REGISTRY_URL,
        'enable.idempotence': True,
        'acks': 'all',
        'retries': 10,
        'max.in.flight.requests.per.connection': 5,
        'linger.ms': linger_ms,
        'compression.type': compression_type,
    }
    producer_config['transactional.id'] = f'benchmark-producer-{transaction_size}-{compression_type}-{linger_ms}-{random.randint(0, 100000)}'
    
    # 테스트 정보 출력
    print(f"\n{'=' * 50}")
    print(f"🧪 벤치마크 테스트 시작: transaction_size={transaction_size}, compression={compression_type}")
    print(f"💡 메시지 수: {num_events:,}, linger.ms: {linger_ms}ms")
    print(f"{'=' * 50}")
    
    avro_producer = AvroProducer(
        producer_config,
        default_value_schema=AVRO_SCHEMA_STRING
    )
    
    start_time = time.time()
    produced_count = 0
    messages_in_batch = []
    
    # 트랜잭션 수
    transaction_count = 0
    
    # 트랜잭션당 평균 시간을 계산하기 위한 변수
    transaction_times = []
    
    try:
        avro_producer.init_transactions()
        print("✅ 트랜잭션 프로듀서 초기화 완료")
        
        for i in range(num_events):
            msg_payload = make_event_payload(fake, mongo_contents_data)
            messages_in_batch.append(msg_payload)
            
            # 배치가 꽉 차거나 마지막 메시지일 경우 트랜잭션 처리
            if len(messages_in_batch) >= transaction_size or i == num_events - 1:
                try:
                    tx_start_time = time.time()
                    avro_producer.begin_transaction()
                    
                    for msg in messages_in_batch:
                        avro_producer.produce(topic=KAFKA_TOPIC_AVRO, value=msg, callback=delivery_report)
                        produced_count += 1
                    
                    # 트랜잭션 커밋
                    avro_producer.commit_transaction()
                    tx_end_time = time.time()
                    tx_duration = tx_end_time - tx_start_time
                    transaction_times.append(tx_duration)
                    
                    transaction_count += 1
                    
                    # 기본 진행 상황 업데이트
                    if transaction_count % 5 == 0 or i == num_events - 1:
                        elapsed = time.time() - start_time
                        msgs_per_sec = produced_count / elapsed if elapsed > 0 else 0
                        print(f"🚀 트랜잭션 {transaction_count} 완료: {len(messages_in_batch)}개 메시지, {tx_duration:.2f}초 소요 (현재: {produced_count:,}/{num_events:,}, {msgs_per_sec:.1f} msgs/sec)")
                    
                    messages_in_batch = []
                
                except ConfluentKafkaError as e:
                    print(f"❌ 트랜잭션 실패: {e}. 트랜잭션 중단.")
                    avro_producer.abort_transaction()
                    messages_in_batch = []
                    if e.code() == ConfluentKafkaError._FATAL:
                        raise e
                
                except Exception as e:
                    print(f"❌ 트랜잭션 중 예상치 못한 오류: {e}. 트랜잭션 중단.")
                    avro_producer.abort_transaction()
                    messages_in_batch = []
                    raise e
            
            # 주기적으로 poll() 호출하여 콜백 처리 및 버퍼 관리
            if i % 10000 == 0:
                avro_producer.poll(0)
    
    except Exception as e:
        print(f"❌ 벤치마크 테스트 중 오류: {e}")
        return None
    
    finally:
        print(f"⏳ 남은 메시지 전송 중 ({len(avro_producer)} 메시지 대기 중)...")
        remaining_messages = avro_producer.flush(timeout=30)
        if remaining_messages > 0:
            print(f"⚠️ 최종 flush 후에도 {remaining_messages}개 메시지가 대기 중입니다.")
    
    end_time = time.time()
    total_duration = end_time - start_time
    avg_transaction_time = sum(transaction_times) / len(transaction_times) if transaction_times else 0
    throughput = produced_count / total_duration if total_duration > 0 else 0
    
    # 결과 출력
    print(f"\n{'=' * 50}")
    print(f"📊 벤치마크 결과: transaction_size={transaction_size}, compression={compression_type}")
    print(f"⏱️ 총 실행 시간: {total_duration:.2f}초")
    print(f"📦 총 메시지 수: {produced_count:,}")
    print(f"🔄 총 트랜잭션 수: {transaction_count}")
    print(f"⚡ 초당 메시지 처리량: {throughput:.2f} msgs/sec")
    print(f"⏱️ 평균 트랜잭션 시간: {avg_transaction_time*1000:.2f}ms")
    print(f"{'=' * 50}")
    
    # 성능 데이터 반환
    return {
        'transaction_size': transaction_size,
        'compression_type': compression_type,
        'linger_ms': linger_ms,
        'total_messages': produced_count,
        'total_transactions': transaction_count,
        'total_duration_sec': round(total_duration, 2),
        'throughput_msgs_sec': round(throughput, 2),
        'avg_transaction_time_ms': round(avg_transaction_time * 1000, 2)
    }

def run_scenario(scenario_name, num_events, transaction_sizes, compression_types, linger_ms_values):
    """시나리오에 따라 여러 설정으로 벤치마크를 실행하는 함수"""
    print(f"\n🧪 시나리오 '{scenario_name}' 테스트 시작 (이벤트 수: {num_events:,})")
    
    results = []
    
    for transaction_size in transaction_sizes:
        for compression_type in compression_types:
            for linger_ms in linger_ms_values:
                print(f"\n🔍 테스트 조합: transaction_size={transaction_size}, compression={compression_type}, linger_ms={linger_ms}")
                
                result = run_benchmark(transaction_size, compression_type, num_events, linger_ms)
                if result:
                    result['scenario'] = scenario_name
                    results.append(result)
                
                # 테스트 간 1초 대기 (서버 부하 완화)
                time.sleep(1)
    
    # CSV 파일로 결과 저장
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"kafka_benchmark_{scenario_name}_{timestamp}.csv"
    
    if results:
        with open(filename, 'w', newline='') as csvfile:
            fieldnames = results[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for result in results:
                writer.writerow(result)
        
        print(f"\n✅ 결과가 '{filename}' 파일에 저장되었습니다.")
    
    return results

def main():
    parser = argparse.ArgumentParser(description='Kafka 트랜잭션 성능 벤치마크')
    parser.add_argument('--scenario', type=str, choices=['normal', 'weekend', '특별이벤트', '모든시나리오'], default='normal',
                        help='테스트할 시나리오 (normal, weekend, 특별이벤트, 모든시나리오)')
    args = parser.parse_args()
    
    # 시나리오 설정
    scenarios = {
        'normal': {
            'name': '일반 평일',
            'events': 300000,  # 피크 시간 2시간 (약 42 msg/sec)
            'transaction_sizes': [500, 1000, 2500, 5000, 10000],
            'compression_types': ['none', 'gzip', 'snappy', 'lz4'],
            'linger_ms': [100, 200]
        },
        'weekend': {
            'name': '주말 피크',
            'events': 400000,  # 피크 시간 2시간 (약 56 msg/sec)
            'transaction_sizes': [500, 1000, 2500, 5000, 10000],
            'compression_types': ['none', 'gzip', 'snappy', 'lz4'],
            'linger_ms': [100, 200]
        },
        '특별이벤트': {
            'name': '신작 출시일',
            'events': 500000,  # 피크 시간 2시간 (약 125 msg/sec)
            'transaction_sizes': [500, 1000, 2500, 5000, 10000],
            'compression_types': ['none', 'gzip', 'snappy', 'lz4'],
            'linger_ms': [50, 100, 200]
        }
    }
    
    # 테스트 실행
    all_results = []
    
    if args.scenario == '모든시나리오':
        for scenario_id, config in scenarios.items():
            results = run_scenario(config['name'], config['events'], 
                                  config['transaction_sizes'], 
                                  config['compression_types'],
                                  config['linger_ms'])
            all_results.extend(results)
    else:
        config = scenarios[args.scenario]
        results = run_scenario(config['name'], config['events'], 
                              config['transaction_sizes'], 
                              config['compression_types'],
                              config['linger_ms'])
        all_results.extend(results)
    
    # 결과 요약
    if all_results:
        # 처리량 기준 상위 3개 조합 출력
        print("\n🏆 처리량 기준 상위 3개 설정:")
        top_throughput = sorted(all_results, key=lambda x: x['throughput_msgs_sec'], reverse=True)[:3]
        for i, result in enumerate(top_throughput):
            print(f"{i+1}위: transaction_size={result['transaction_size']}, "
                  f"compression={result['compression_type']}, "
                  f"linger_ms={result['linger_ms']}, "
                  f"처리량={result['throughput_msgs_sec']} msgs/sec")
        
        # 트랜잭션 시간 기준 상위 3개 조합 출력
        print("\n⏱️ 평균 트랜잭션 시간 기준 상위 3개 설정 (낮을수록 좋음):")
        top_tx_time = sorted(all_results, key=lambda x: x['avg_transaction_time_ms'])[:3]
        for i, result in enumerate(top_tx_time):
            print(f"{i+1}위: transaction_size={result['transaction_size']}, "
                  f"compression={result['compression_type']}, "
                  f"linger_ms={result['linger_ms']}, "
                  f"평균 트랜잭션 시간={result['avg_transaction_time_ms']}ms")

if __name__ == "__main__":
    main()

# 테스트 명령어
# # 일반 평일 시나리오만 테스트
# python kafka_transaction_benchmark.py --scenario normal

# # 주말 피크 시나리오만 테스트
# python kafka_transaction_benchmark.py --scenario weekend

# # 특별 이벤트 시나리오만 테스트
# python kafka_transaction_benchmark.py --scenario 특별이벤트

# # 모든 시나리오 테스트
# python kafka_transaction_benchmark.py --scenario 모든시나리오