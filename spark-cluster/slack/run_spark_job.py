#!/usr/bin/env python3
import os
import yaml
import argparse
import subprocess
import sys

def load_config(config_file):
    """YAML 설정 파일을 로드하고 환경 변수를 확장"""
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    
    # 환경 변수 확장
    process_env_vars(config)
    return config

def process_env_vars(config):
    """설정 딕셔너리에서 환경 변수 참조를 실제 값으로 치환"""
    if isinstance(config, dict):
        for key, value in config.items():
            if isinstance(value, (dict, list)):
                process_env_vars(value)
            elif isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                env_var = value[2:-1]
                env_value = os.environ.get(env_var)
                if env_value is None:
                    print(f"경고: 환경 변수 '{env_var}'가 설정되지 않았습니다.")
                config[key] = env_value
    elif isinstance(config, list):
        for i, item in enumerate(config):
            if isinstance(item, (dict, list)):
                process_env_vars(item)
            elif isinstance(item, str) and item.startswith("${") and item.endswith("}"):
                env_var = item[2:-1]
                env_value = os.environ.get(env_var)
                if env_value is None:
                    print(f"경고: 환경 변수 '{env_var}'가 설정되지 않았습니다.")
                config[i] = env_value

def build_spark_submit_command(config, job_script):
    """설정 파일에서 spark-submit 명령어 생성"""
    cmd = [
        f"{os.environ.get('SPARK_HOME', '')}/bin/spark-submit" if os.environ.get('SPARK_HOME') else "spark-submit",
        "--master", config['spark']['master']
    ]
    
    # 패키지 추가
    if config['spark'].get('packages'):
        cmd.extend(["--packages", ",".join(config['spark']['packages'])])
    
    # 스크립트 경로 추가
    cmd.append(job_script)
    
    # 스크립트 인자 추가
    cmd.extend([
        "--schema_registry_url", config['schema_registry']['url'],
        "--schema_subject", config['schema_registry']['subject'],
        "--kafka_brokers", config['kafka']['brokers'],
        "--kafka_topic", config['kafka']['topic'],
        "--iceberg_catalog_name", config['iceberg']['catalog_name'],
        "--iceberg_warehouse_path", config['iceberg']['warehouse_path'],
        "--iceberg_db_name", config['iceberg']['db_name'],
        "--iceberg_table_name", config['iceberg']['table_name'],
        "--checkpoint_location", config['iceberg']['checkpoint_location'],
        "--s3_endpoint", config['s3']['endpoint'],
        "--s3_access_key", config['s3']['access_key'],
        "--s3_secret_key", config['s3']['secret_key'],
        "--processing_time_trigger", config['streaming']['processing_time_trigger']
    ])

    # 스키마 버전 관리 인자 추가
    if config.get('schema_version_state'):
        cmd.extend([
            "--schema_version_s3_bucket", config['schema_version_state']['s3_bucket'],
            "--schema_version_s3_key", config['schema_version_state']['s3_key']
        ])
    
    # 슬랙 웹훅 URL 인자 추가 (선택적)
    if config.get('slack') and config['slack'].get('webhook_url'):
        cmd.extend(["--slack_webhook_url", config['slack']['webhook_url']])
    
    return cmd

def main():
    parser = argparse.ArgumentParser(description="Spark 작업 실행 스크립트")
    parser.add_argument("--config", default="config.yaml", help="설정 파일 경로 (기본값: config.yaml)")
    parser.add_argument("--job", default="schema_slack_iceberg.py", help="실행할 Spark 작업 스크립트 (기본값: schema_slack_iceberg.py")
    args = parser.parse_args()
    
    # .env 파일이 있으면 로드 (python-dotenv 사용)
    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("환경 변수를 .env 파일에서 로드했습니다.")
    except ImportError:
        print("python-dotenv가 설치되지 않았습니다. 환경 변수를 시스템에서만 로드합니다.")
    
    # 설정 로드
    config = load_config(args.config)
    
    # 커맨드 생성
    cmd = build_spark_submit_command(config, args.job)
    
    # 커맨드 출력 (민감 정보는 숨김)
    safe_cmd = cmd.copy()
    for i, arg in enumerate(safe_cmd):
        if arg in ["--s3_access_key", "--s3_secret_key"] and i+1 < len(safe_cmd):
            safe_cmd[i+1] = "****"
    print(f"실행 명령어: {' '.join(safe_cmd)}")
    
    # 작업 실행
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"작업 실행 실패: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("작업이 사용자에 의해 중단되었습니다.")
        sys.exit(130)

if __name__ == "__main__":
    main()