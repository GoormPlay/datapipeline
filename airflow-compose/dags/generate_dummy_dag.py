from airflow import DAG
from airflow.operators.python import PythonOperator

import random
import time
from datetime import datetime, timedelta

from faker import Faker

# confluent_kafka 및 Avro 관련 import 추가
from confluent_kafka.avro import AvroProducer
from confluent_kafka import KafkaError as ConfluentKafkaError # KafkaError 이름 충돌 방지

# MongoDB 연결을 위한 pymongo import
try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure
    PYMONGO_AVAILABLE = True
except ImportError:
    PYMONGO_AVAILABLE = False
    MongoClient = None
    ConnectionFailure = None

from utils.slack_fail_noti import task_fail_slack_alert

kafka_cluster = '43.201.43.88:9092,15.165.234.219:9092,3.35.228.177:9092'
SCHEMA_REGISTRY_URL = 'http://43.201.43.88:8081' # Schema Registry URL
KAFKA_TOPIC_AVRO = 'userlog-avro-topic'         # Avro 메시지를 위한 Kafka 토픽

# Avro 스키마 정의 (make_event 함수 구조 기반)
AVRO_SCHEMA_STRING = """
{
    "type": "record",
    "name": "UserEvent",
    "namespace": "com.example.airflow.dummy",
    "fields": [
        {"name": "videoId", "type": ["null", "string"], "default": null},
        {"name": "title", "type": ["null", "string"], "default": null},
        {"name": "userId", "type": "string"},
        {"name": "timestamp", "type": "string"},
        {"name": "eventType", "type": "string"},
        {"name": "page", "type": "string"},
        {"name": "liked", "type": ["null", "boolean"], "default": null},
        {"name": "review", "type": ["null", "string"], "default": null},
        {"name": "rating", "type": ["null", "int"], "default": null},
        {"name": "contentCategory", "type": ["null", {"type": "array", "items": "string"}], "default": null}
    ]
}
"""

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        # Log a small percentage of successful deliveries to avoid excessive logging
        if random.random() < 0.0001: # Log 0.01% of successful messages
             print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def generate_event_avro(**kwargs):
    fake = Faker()

    producer_config = {
        'bootstrap.servers': kafka_cluster,
        'schema.registry.url': SCHEMA_REGISTRY_URL,
        'retries': 10,
        'linger.ms': 200,
    }

    avro_producer = AvroProducer(
        producer_config,
        default_value_schema=AVRO_SCHEMA_STRING
    )

    mongo_contents_data = []
    if PYMONGO_AVAILABLE:
        try:
            # MongoDB 연결 정보 - 실제 환경에 맞게 수정하세요.
            # 예: client = MongoClient('mongodb://user:pass@host:port/admin')
            client = MongoClient('mongodb+srv://user:goorm0508@goorm-mongodb.svz66jf.mongodb.net/?retryWrites=true&w=majority&appName=goorm-mongoDB') # 로컬 MongoDB 예시
            client.admin.command('ping') # 연결 테스트
            db = client['content-db']
            contents_collection = db['contents']
            # 'title'과 'videoId' 필드만 가져옵니다. _id는 제외합니다.
            # 실제 MongoDB의 필드명이 'videoId'가 아니라면 해당 필드명으로 수정해야 합니다.
            mongo_contents_data = list(contents_collection.find({}, {"_id": 0, "title": 1, "videoId": 1}))
            client.close()
            if mongo_contents_data:
                print(f"✅ Successfully fetched {len(mongo_contents_data)} items from MongoDB 'contents' collection.")
            else:
                print("ℹ️ No data fetched from MongoDB 'contents' collection or collection is empty.")
        except ConnectionFailure:
            print("❌ Failed to connect to MongoDB. Will proceed without MongoDB data.")
        except Exception as e:
            print(f"❌ Error fetching data from MongoDB: {e}. Will proceed without MongoDB data.")

    num_events = 1_000_000  # 필요한 양으로 조절 가능
    # num_events = 1000 # 테스트용

    event_types = ["like_click", "content_click", "review_write", "rating_submit"]
    pages = ["content_detail", "main"]

    def make_event_payload(): # 함수명 변경하여 명확화
        event = {}
        if mongo_contents_data:
            selected_content = random.choice(mongo_contents_data)
            event["videoId"] = selected_content.get("videoId")
            event["title"] = selected_content.get("title")
        else:
            event["videoId"] = None
            event["title"] = None # Avro 스키마에 따라 null 허용

        event.update({
            "userId": fake.uuid4(),
            "timestamp": fake.date_time_between(start_date="-1d", end_date="now").isoformat() + "Z",
            "eventType": random.choice(event_types),
            "page": random.choice(pages),
        })

        if event["eventType"] == "like_click":
            event["liked"] = random.choice([True, False])
        elif event["eventType"] == "review_write":
            event["review"] = fake.sentence()
        elif event["eventType"] == "rating_submit":
            event["rating"] = random.randint(1, 5)
        elif event["eventType"] == "content_click": # 'else' 대신 명시적으로 'content_click' 처리
            event["contentCategory"] = [fake.word() for _ in range(random.randint(1, 3))]
        else: # 혹시 모를 다른 eventType (현재는 없지만)
            pass
        return event

    print(f"🚀 Producing {num_events:,} dummy Avro messages to Kafka topic `{KAFKA_TOPIC_AVRO}`")

    start_time = time.time()
    produced_count = 0
    for i in range(num_events):
        msg_payload = make_event_payload()
        try:
            # AvroProducer는 value에 dict를 전달하면 스키마에 따라 직렬화
            avro_producer.produce(topic=KAFKA_TOPIC_AVRO, value=msg_payload, callback=delivery_report)
            produced_count += 1
        except BufferError:
            print("Local producer queue is full... flushing pending messages.")
            avro_producer.flush() # 버퍼가 꽉 차면 flush
            # 재시도
            avro_producer.produce(topic=KAFKA_TOPIC_AVRO, value=msg_payload, callback=delivery_report)
            produced_count += 1
        except Exception as e:
            print(f"❌ Error producing message: {e}")

        if i % 10000 == 0:
            avro_producer.poll(0)

    print(f"⏳ Flushing remaining messages ({len(avro_producer)} messages in queue)...")
    remaining_messages = avro_producer.flush(timeout=30)
    if remaining_messages > 0:
        print(f"⚠️ {remaining_messages} messages still in queue after flush timeout.")
    
    end_time = time.time()
    print(f"✅ Sent {produced_count:,} Avro events in {end_time - start_time:.2f} seconds to topic '{KAFKA_TOPIC_AVRO}'")
    print("✅ Dummy Avro events generation process finished.")


with DAG(
    'generate_dummy_avro', # DAG ID 변경
    default_args={
        'depends_on_past':False,
        'retries':1, # 대량 데이터 생성 실패 시 재시도 부담 줄임
        'retry_delay':timedelta(minutes=5),
        'execution_timeout':timedelta(minutes=360), # 실행 시간은 유지
    },
    description="Generates dummy user log data in Avro format to Kafka", # 설명 변경
    start_date=datetime(2025, 6, 12),
    catchup=False,
    schedule_interval='0 * * * *', # 스케줄 변경 (예시)
    tags=['dummy', 'avro', 'kafka'] # 태그 변경
) as dag:
    
    produce_dummy_avro_data = PythonOperator(
        task_id='produce_dummy_avro_data', # 태스크 ID 변경
        python_callable=generate_event_avro,
        on_failure_callback=task_fail_slack_alert
    )

    produce_dummy_avro_data