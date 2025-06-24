from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import random
import time
from io import StringIO, BytesIO
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from faker import Faker

# PyArrow import for Parquet
import pyarrow as pa
import pyarrow.parquet as pq

from kafka import KafkaAdminClient
from kafka.errors import KafkaError

# Confluent Kafka 및 Avro 관련 import
from confluent_kafka import Producer as ConfluentProducer, Consumer as ConfluentConsumer, KafkaError as ConfluentKafkaError, TopicPartition
from confluent_kafka.avro import AvroProducer, AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.admin import AdminClient as ConfluentAdminClient # AdminClient 임포트 추가
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer # 직접 사용할 경우 필요


from utils.slack_fail_noti import task_fail_slack_alert

kafka_cluster = '3.38.160.234:9092,43.202.111.59:9092,43.202.163.215:9092'
SCHEMA_REGISTRY_URL = 'http://3.38.160.234:8081' # Schema Registry URL
KAFKA_TOPIC_AVRO = 'userlog-avro-topic'         # Avro 메시지를 위한 Kafka 토픽

# Avro 스키마 정의 (make_event 함수 구조 기반)
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
        'enable.idempotence': True, # 멱등성 설정
        'acks': 'all', # 멱등성을 위해 'all' 또는 '-1'
        'retries': 10,
        # 멱등성 보장을 위한 설정: ACK 응답 대기 요청을 최대 5개까지만 생성
        # 멱등성 보장에 따른 비효율 발생을 줄임
        'max.in.flight.requests.per.connection': 5,
        'linger.ms': 200,
    }

    topic_configs={
        'min.insync.replicas': 2
    }

    avro_producer = AvroProducer(
        producer_config,
        topic_configs,
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
            mongo_contents_data = list(contents_collection.find({}, {"_id": 0, "title": 1, "videoId": 1, "genre": 1}))
            client.close()
            if mongo_contents_data:
                print(f"✅ Successfully fetched {len(mongo_contents_data)} items from MongoDB 'contents' collection.")
            else:
                print("ℹ️ No data fetched from MongoDB 'contents' collection or collection is empty.")
        except ConnectionFailure:
            print("❌ Failed to connect to MongoDB. Will proceed without MongoDB data.")
        except Exception as e:
            print(f"❌ Error fetching data from MongoDB: {e}. Will proceed without MongoDB data.")

    num_events = 1_00_000  # 필요한 양으로 조절 가능
    # num_events = 1000 # 테스트용

    event_types = ["like_click", "content_click", "review_write", "rating_submit", "paly_start", "paly_stop", "content_recom_click"]
    pages = ["content_detail", "main", "content_paly"]

    def make_event_payload(): # 함수명 변경하여 명확화
        # Avro 스키마에 정의된 모든 필드를 초기에 None으로 설정 (userId, timestamp 등은 아래에서 덮어쓰여짐)
        event = {
            "videoId": None,
            "title": None,
            "userId": None, # Placeholder, will be overwritten
            "timestamp": None, # Placeholder, will be overwritten
            "eventType": None, # Placeholder, will be overwritten
            "page": None, # Placeholder, will be overwritten
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

        # videoId와 title은 mongo_contents_data가 없으면 이미 None으로 설정됨
       # ISO 문자열 대신 Unix 타임스탬프(밀리초)로 설정
        fake_datetime = fake.date_time_between(start_date="-1d", end_date="now")
        unix_timestamp = int(fake_datetime.timestamp() * 1000)  # 초를 밀리초로 변환

        event.update({ # 필수 필드 및 기본값 없는 필드 업데이트
                "userId": fake.uuid4(),
                "timestamp": unix_timestamp,  # 문자열에서 정수로 변경
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
        elif event["eventType"] == "content_click": # 'else' 대신 명시적으로 'content_click' 처리
            event["page"] = "content_detail"
        elif event["eventType"] == "content_recom_click":
            event["page"] = "content_detail"
            event["recMovieList"] = fake.sentence()
        elif event["eventType"] == "paly_start":
            event["page"] = "content_paly"
        elif event["eventType"] == "paly_stop":
            event["page"] = "content_paly"

        # 다른 eventType의 경우, 해당 특정 필드들은 None으로 유지됨
        return event

    print(f"🚀 Producing {num_events:,} dummy Avro messages to Kafka topic `{KAFKA_TOPIC_AVRO}`")

    start_time = time.time()
    produced_count = 0 # 총 생산된 메시지 수
    transaction_size = 1000 # 트랜잭션당 메시지 수 (이 값을 조절하여 성능 테스트 가능)
    messages_in_batch = [] # 현재 배치에 포함된 메시지

    try:
        # 1. initTransaction(): 트랜잭션 준비
        avro_producer.init_transactions()
        print("✅ Transactional producer initialized.")

        for i in range(num_events):
            msg_payload = make_event_payload()
            messages_in_batch.append(msg_payload)

            if len(messages_in_batch) >= transaction_size or i == num_events - 1:
                try:
                    # 2. beginTransaction(): 트랜잭션 시작
                    avro_producer.begin_transaction()
                    for msg in messages_in_batch:
                        # 3. send(): 트랜잭션 내 레코드 배치 생성
                        avro_producer.produce(topic=KAFKA_TOPIC_AVRO, value=msg, callback=delivery_report)
                        produced_count += 1
                    # 4. commitTransaction(): flush 동작을 포함
                    avro_producer.commit_transaction()
                    print(f"✅ Committed {len(messages_in_batch)} messages in a transaction. Total produced: {produced_count}")
                    messages_in_batch = [] # 배치 초기화
                except KafkaError as e:
                    print(f"❌ Transaction failed: {e}. Aborting transaction.")
                    avro_producer.abort_transaction()
                    # 트랜잭션 실패 시, 해당 배치 메시지는 다시 시도하지 않음 (재처리 로직 필요시 추가)
                    messages_in_batch = []
                    # 심각한 오류의 경우 DAG 실패 처리
                    if e.code() == ConfluentKafkaError.FATAL:
                        raise e
                except Exception as e:
                    print(f"❌ Unexpected error during transaction: {e}. Aborting transaction.")
                    avro_producer.abort_transaction()
                    messages_in_batch = []
                    raise e

            # 주기적으로 poll() 호출하여 콜백 처리 및 버퍼 관리
            if i % 1000 == 0: # 1000 메시지마다 poll
                avro_producer.poll(0)

    except Exception as e:
        print(f"❌ Error during event generation: {e}")
        raise e
    finally:
        # Ensure any remaining messages in the producer buffer are flushed
        # This is important for non-transactional producers or if the last transaction was small
        # For transactional producers, commit_transaction() handles flushing.
        # However, if an error occurred before begin_transaction or after abort_transaction,
        # there might be messages in the buffer that need to be flushed.
        # Confluent Kafka's AvroProducer.flush() will also handle pending transactional messages.
        print(f"⏳ Flushing remaining messages ({len(avro_producer)} messages in queue)...")
        remaining_messages = avro_producer.flush(timeout=30)
        if remaining_messages > 0:
            print(f"⚠️ {remaining_messages} messages still in queue after final flush timeout.")
        
        end_time = time.time()
        print(f"✅ Sent {produced_count:,} Avro events in {end_time - start_time:.2f} seconds to topic '{KAFKA_TOPIC_AVRO}'")
        print("✅ Dummy Avro events generation process finished.")

    print(f"⏳ Flushing remaining messages ({len(avro_producer)} messages in queue)...")
    remaining_messages = avro_producer.flush(timeout=30)
    if remaining_messages > 0:
        print(f"⚠️ {remaining_messages} messages still in queue after flush timeout.")
    
    end_time = time.time()
    print(f"✅ Sent {produced_count:,} Avro events in {end_time - start_time:.2f} seconds to topic '{KAFKA_TOPIC_AVRO}'")
    print("✅ Dummy Avro events generation process finished.")



def connect_minio():
    s3_hook = S3Hook(
        aws_conn_id='minio',
        region_name='us-east-1' # MinIO는 region 개념이 없지만, S3Hook은 필요로 함
    )
    return s3_hook

def kafka_consumer(**context):
    consumer = None
    processed_messages_in_session = 0 # 이번 태스크 실행에서 처리된 총 메시지 수
    # 각 파티션별로 커밋할 오프셋을 저장하는 딕셔너리
    # key: TopicPartition(topic, partition), value: offset (커밋할 다음 메시지의 오프셋)
    offsets_to_commit_map = {}

    try:
        consumer_config = {
            'bootstrap.servers': kafka_cluster, # 실제 Kafka 브로커 주소
            'group.id': 'avro-userlog-consumer-group-01', # 컨슈머 그룹 ID
            'schema.registry.url': SCHEMA_REGISTRY_URL,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False, # 수동 커밋
            # 'fetch.min.bytes': 1, # 기본값
            # 'fetch.max.wait.ms': 500, # 기본값
            # 'max.poll.records': 500, # confluent-kafka에서는 이 설정이 직접적으로 poll() 반환 개수를 제어하지 않음
                                     # poll()은 한 번에 여러 메시지를 가져올 수 있지만, 반환은 하나씩 또는 에러/타임아웃
        }
        # AvroConsumer는 value_schema를 명시적으로 전달할 필요 없음 (Schema Registry에서 가져옴)
        consumer = AvroConsumer(consumer_config)
        consumer.subscribe([KAFKA_TOPIC_AVRO])
        print(f"✅ AvroConsumer subscribed to topic '{KAFKA_TOPIC_AVRO}' with group ID '{consumer_config['group.id']}'")

        last_message_received_time = time.time()
        POLL_TIMEOUT_S = 1.0  # poll 타임아웃 (초 단위)
        IDLE_CONSUMPTION_TIMEOUT_S = 30 # 마지막 메시지 수신 후 이 시간 동안 메시지 없으면 종료 (초)
        MAX_COLLECTION_DURATION_S = 300 # 최대 실행 시간 (초)
        collection_start_time = time.time()

        print(f"🚀 Starting message collection for up to {MAX_COLLECTION_DURATION_S}s or until idle for {IDLE_CONSUMPTION_TIMEOUT_S}s.")

        s3_hook = connect_minio()
        print("✅ MinIO connected")
        bucket_name = "userlog-data" # MinIO 버킷 이름
        if not s3_hook.check_for_bucket(bucket_name):
            print(f"Bucket '{bucket_name}' does not exist. Creating...")
            s3_hook.create_bucket(bucket_name=bucket_name)
            print(f"✅ Bucket '{bucket_name}' created.")
        else:
            print(f"✅ Bucket '{bucket_name}' already exists.")

        current_time_kst = datetime.now(ZoneInfo("Asia/Seoul"))
        base_filename = current_time_kst.strftime("%Y-%m-%d_%H-%M-%S")
         # 파일명을 Parquet으로 변경하고 S3 경로도 업데이트
        parquet_filename = f"{base_filename}_avro_consumed.parquet"
        s3_parquet_object_key = f"test/user-activity-raw-parquet/{parquet_filename}" # S3 경로도 Parquet으로 명시

        collected_events = [] # 역직렬화된 이벤트를 저장할 리스트

         # 로컬 JSON 파일 생성 로직 대신, 리스트에 메시지 수집
        # with open(local_json_filename, "w") as f: # 이 부분 제거
        while True:
            # 최대 실행 시간 초과 확인
            if time.time() - collection_start_time > MAX_COLLECTION_DURATION_S:
                print(f"ℹ️ Max collection duration of {MAX_COLLECTION_DURATION_S}s reached.")
                break

            msg = consumer.poll(timeout=POLL_TIMEOUT_S)

            if msg is None: # 타임아웃, 메시지 없음
                if time.time() - last_message_received_time > IDLE_CONSUMPTION_TIMEOUT_S:
                    print(f"ℹ️ No messages received for {IDLE_CONSUMPTION_TIMEOUT_S}s. Finalizing batch.")
                    break
                continue # 아직 idle 타임아웃에 도달하지 않았으면 계속 폴링

            if msg.error():
                if msg.error().code() == ConfluentKafkaError._PARTITION_EOF:
                    print(f"ℹ️ Reached end of partition for {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                    continue
                elif msg.error().code() == ConfluentKafkaError.UNKNOWN_TOPIC_OR_PART:
                    print(f"❌ Error: Unknown topic or partition: {msg.error()}. Waiting for metadata refresh...")
                    time.sleep(5) # 잠시 대기 후 재시도 (메타데이터 갱신 시간 부여)
                    continue
                else:
                    print(f"❌ Consumer error: {msg.error()}")
                    raise Exception(f"Consumer error: {msg.error()}")

            last_message_received_time = time.time() # 메시지 수신 시간 갱신
            try:
                event_data = msg.value() # Avro 역직렬화된 Python dict
                if event_data is None: # Tombstone 메시지 등 value가 null인 경우
                    print(f"ℹ️ Received a message with null value (possibly tombstone) at {msg.topic()} [{msg.partition()}] offset {msg.offset()}. Skipping.")
                    tp_key = (msg.topic(), msg.partition())
                    offsets_to_commit_map[tp_key] = msg.offset() + 1
                    continue

                # JSON 파일에 쓰는 대신 리스트에 추가
                collected_events.append(event_data)
                processed_messages_in_session += 1

                # 커밋할 오프셋 업데이트 (파티션별로 다음 메시지의 오프셋)
                tp_key = (msg.topic(), msg.partition())
                offsets_to_commit_map[tp_key] = msg.offset() + 1

                if processed_messages_in_session % 100 == 0: # 예: 100개 메시지 처리마다 로그 출력
                    print(f"📝 Processed {processed_messages_in_session} messages so far in this session...")

            except SerializerError as e:
                print(f"❌ Message deserialization failed for message at offset {msg.offset()}: {e}. Skipping.")
                tp_key = (msg.topic(), msg.partition())
                offsets_to_commit_map[tp_key] = msg.offset() + 1
                continue # 다음 메시지 처리
            except Exception as e:
                print(f"❌ Failed to process message (value: {msg.value() if msg else 'N/A'}) for writing: {e}")
                raise e

        if processed_messages_in_session == 0:
            print("ℹ️ No messages processed during the collection period.")
            # 로컬 파일 관련 로직 제거
            print("✅ No messages to process. Task finished gracefully.")
            context['ti'].xcom_push(key='s3_object_key', value=None) # XCom 키 통일
            context['ti'].xcom_push(key='base_filename_for_processed', value=None)
            return # 정상 종료

        print(f"✅ Collected {processed_messages_in_session} messages. Converting to Parquet and uploading to MinIO.")

        # 수집된 이벤트를 PyArrow Table로 변환 후 Parquet으로 MinIO에 업로드
        if collected_events:
            try:
                arrow_table = pa.Table.from_pylist(collected_events)
                
                parquet_buffer = BytesIO()
                pq.write_table(arrow_table, parquet_buffer)
                parquet_buffer.seek(0)

                s3_hook.load_file_obj(parquet_buffer, s3_parquet_object_key, bucket_name, replace=True)
                print(f"✅ Parquet file {parquet_filename} uploaded to MinIO: s3://{bucket_name}/{s3_parquet_object_key}")
                
                # XCom PUSH (Parquet 경로)
                context['ti'].xcom_push(key='s3_object_key', value=s3_parquet_object_key) # XCom 키 통일
                context['ti'].xcom_push(key='base_filename_for_processed', value=base_filename)

            except Exception as e:
                print(f"❌ Failed to convert to Parquet or upload to MinIO: {e}")
                # Parquet 변환/업로드 실패 시 XCom 값 설정 방지 또는 에러에 따른 처리
                context['ti'].xcom_push(key='s3_object_key', value=None)
                context['ti'].xcom_push(key='base_filename_for_processed', value=None)
                raise e
        else: # processed_messages_in_session > 0 이지만 collected_events가 비어있는 경우는 거의 없으나 방어적으로 처리
            print("ℹ️ No events in collected_events list, skipping Parquet conversion and upload.")
            context['ti'].xcom_push(key='s3_object_key', value=None)
            context['ti'].xcom_push(key='base_filename_for_processed', value=None)


        # 오프셋 커밋 (이 부분은 변경 없음)
        if offsets_to_commit_map:
            commit_list = [TopicPartition(topic, partition, offset) for (topic, partition), offset in offsets_to_commit_map.items()]
            try:
                consumer.commit(offsets=commit_list, asynchronous=False) # 동기 커밋
                print(f"✅ Successfully committed offsets for {len(commit_list)} partitions.")
                for tp_offset in commit_list:
                    print(f"  - Topic: {tp_offset.topic}, Partition: {tp_offset.partition}, Offset: {tp_offset.offset}")
            except Exception as e:
                print(f"❌ Failed to commit offsets: {e}")
                raise e
        else:
            print("ℹ️ No offsets to commit.")

        # XCom PUSH 로직은 Parquet 업로드 성공 시 이미 수행됨

    except Exception as e:
        print(f"❌ Kafka consumer task failed: {e}")
        # 실패 시에도 XCom을 None으로 설정하여 후속 작업이 오동작하지 않도록 할 수 있음
        context['ti'].xcom_push(key='s3_object_key', value=None)
        context['ti'].xcom_push(key='base_filename_for_processed', value=None)
        raise e
    finally:
        if consumer:
            print("ℹ️ Closing Kafka consumer.")
            consumer.close()
        # 로컬 파일 생성/삭제 로직이 없어졌으므로 관련 코드 제거


def check_kafka_broker_health():
    brokers = kafka_cluster.split(',') # Split the comma-separated string into a list of brokers
    alive_count = 0
    # admin_client instance will be created inside the loop for each broker check
    MIN_ALIVE_BROKERS = 2

    for broker_url in brokers:
        # admin_client_instance = None # Not strictly needed as ConfluentAdminClient doesn't have explicit close
        try:
            conf = {
                'bootstrap.servers': broker_url, # 개별 브로커 URL로 설정
                'client.id': f'airflow-health-check-{broker_url.replace(":", "-").replace(".", "_")}', # 유니크한 client.id
                'socket.timeout.ms': 5000,      # 소켓 연결 타임아웃 (ms)
            }
            admin_client = ConfluentAdminClient(conf)
            # list_topics 호출 시 타임아웃(초 단위) 설정
            cluster_metadata = admin_client.list_topics(timeout=5.0)
            print(f"✅ Broker {broker_url} is alive. Cluster ID: {cluster_metadata.cluster_id}. Found {len(cluster_metadata.topics)} topics.")
            alive_count += 1
        except ConfluentKafkaError as e: # ConfluentKafkaError 사용
            print(f"❌ Broker {broker_url} health check failed: {e}")
        except Exception as e:
            print(f"❌ An unexpected error occurred while checking broker {broker_url}: {e}")
        # finally: ConfluentAdminClient는 명시적인 close() 메서드가 필요하지 않습니다.
            # 리소스는 내부적으로 관리됩니다.

    if alive_count < MIN_ALIVE_BROKERS:
        raise Exception(f"Kafka cluster health check failed: Only {alive_count} out of {len(brokers)} brokers are alive. Required: {MIN_ALIVE_BROKERS}.")
    else:
        print(f"✅ Kafka cluster health check passed: {alive_count} brokers are alive.")


with DAG(
    dag_id='avro_userlog_pipeline', 
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2, 
        'retry_delay': timedelta(minutes=1), 
        'execution_timeout': timedelta(minutes=120), 
        'on_failure_callback': task_fail_slack_alert, 
    },
    description="Userlog pipeline: Kafka(Avro) -> MinIO(Parquet) -> Iceberg", # 설명 업데이트
    start_date=datetime(2025, 6, 1), 
    catchup=False, 
    schedule_interval='*/30 * * * *', # 30분 간격으로 실행
    tags=['userlog', 'avro', 'kafka', 'parquet', 'minio', 'iceberg'] # 태그 업데이트
) as dag:
    
    produce_avro_data = PythonOperator(
        task_id='produce_avro_data',
        python_callable=generate_event_avro,
    )
    

    check_kafka_brokers_health = PythonOperator(
        task_id='check_kafka_broker_health',
        python_callable=check_kafka_broker_health,
    )

    consume_avro_data_to_minio = PythonOperator(
        task_id='kafka_consumer_avro_to_parquet_minio', # 태스크 ID 변경
        python_callable=kafka_consumer,
    )

    # check_minio_file_upload = S3KeySensor(
    #     task_id='check_minio_file_upload',
    #     bucket_name='userlog-data', 
    #     bucket_key="{{ ti.xcom_pull(task_ids='kafka_consumer_avro_to_parquet_minio', key='s3_object_key') }}", # XCom 키 및 태스크 ID 수정
    #     aws_conn_id='minio',
    #     poke_interval=30, 
    #     timeout=600, 
    #     soft_fail=False, # Parquet 파일이 반드시 있어야 Iceberg 작업이 의미 있으므로 False로 변경 고려
    # )

    # Spark 작업: MinIO의 Parquet 파일을 읽어 Iceberg 테이블을 생성/업데이트합니다.
    # 실제 Spark 애플리케이션 ('/opt/spark/data/manage_iceberg_table.py')은 이 목적에 맞게 작성되어야 합니다.
    # manage_iceberg_table = SparkSubmitOperator(
    #     task_id='manage_iceberg_table_from_parquet', # 태스크 ID 및 역할 변경
    #     application="/opt/spark/data/userlog_iceberg_spark.py", # Iceberg 처리용 Spark 앱 경로 (예시)
    #     conn_id='spark', 
    #     application_args=[
    #         "--source_parquet_path", f"s3a://userlog-data/{{{{ ti.xcom_pull(task_ids='kafka_consumer_avro_to_parquet_minio', key='s3_object_key') }}}}",
    #         "--iceberg_catalog_name", "minio_catalog",
    #         "--iceberg_db_name", "userlog_db",
    #         "--iceberg_table_name", "user_activity_logs", # 대상 Iceberg 테이블 이름 (예시, 필요시 avro_user_activity_logs 등으로 변경)
    #         "--s3_endpoint", "http://54.180.166.228:9000", # MinIO 엔드포인트
    #         "--s3_access_key", "minioadmin",       # MinIO Access Key
    #         "--s3_secret_key", "minioadmin"        # MinIO Secret Key
    #     ],
    #     # Iceberg 사용을 위해 Spark에 필요한 JAR들을 포함해야 합니다.
    #     # 예: iceberg-spark-runtime, aws-java-sdk-bundle 등
    #     jars="/opt/spark/jars/hadoop-aws-3.3.1.jar,/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar,/opt/spark/jars/iceberg-spark-runtime-3.4_2.12-1.4.2.jar", # 실제 Iceberg JAR 경로로 수정
    # )

    check_kafka_brokers_health >> consume_avro_data_to_minio 
    # >> check_minio_file_upload 
    # >> manage_iceberg_table
