from pyhive import hive
import pandas as pd
import time

# 연결 시도 함수
def try_connect(max_attempts=3, wait_seconds=2):
    attempt = 0
    last_exception = None
    
    while attempt < max_attempts:
        try:
            print(f"연결 시도 {attempt+1}...")
            conn = hive.Connection(
                host='localhost',
                port=10000,
                configuration={
                    'hive.server2.transport.mode': 'binary'
                    # 'hive.server2.thrift.sasl.qop': 'auth'
                }
            )
            print("연결 성공!")
            return conn
        except Exception as e:
            last_exception = e
            print(f"연결 실패: {e}")
            attempt += 1
            if attempt < max_attempts:
                print(f"{wait_seconds}초 후 재시도...")
                time.sleep(wait_seconds)
    
    raise Exception(f"최대 시도 횟수 초과: {last_exception}")

# 연결 시도
conn = try_connect()

# 테스트 쿼리 실행
cursor = conn.cursor()

# 기본 쿼리 실행
try:
    print("\n사용 가능한 카탈로그 조회 중...")
    cursor.execute("SHOW CATALOGS")
    result = cursor.fetchall()
    print("사용 가능한 카탈로그:")
    for row in result:
        print(row[0])
except Exception as e:
    print(f"카탈로그 조회 오류: {e}")

# userlogs_catalog 스키마 조회
try:
    print("\nuserlogs_catalog의 스키마 조회 중...")
    cursor.execute("SHOW SCHEMAS IN userlogs_catalog")
    schemas = cursor.fetchall()
    print("userlogs_catalog의 스키마:")
    for schema in schemas:
        print(schema[0])
    
    # analytics 스키마가 존재하는 경우에만 테이블 조회
    if schemas and any(schema[0] == 'analytics' for schema in schemas):
        print("\nuserlogs_catalog.analytics의 테이블 조회 중...")
        cursor.execute("SHOW TABLES IN userlogs_catalog.analytics")
        tables = cursor.fetchall()
        print("userlogs_catalog.analytics의 테이블:")
        for table in tables:
            print(table[0])
except Exception as e:
    print(f"스키마/테이블 조회 오류: {e}")
finally:
    try:
        cursor.close()
        conn.close()
        print("\n연결 종료 완료")
    except:
        print("연결 종료 중 오류 발생")