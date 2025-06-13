FROM spark:3.5.5-scala2.12-java17-ubuntu

USER root

# Python3 및 의존성 설치
RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip curl wget; \
    rm -rf /var/lib/apt/lists/*

# 필요한 JAR 다운로드
# spark-submit 시 --packages 옵션을 통해 필요한 의존성을 가져오므로 Dockerfile에서 직접 다운로드할 필요가 줄어듭니다.
# 만약 특정 JAR가 모든 작업에 공통적으로 필요하고, --packages로 관리하고 싶지 않다면 여기에 유지할 수 있습니다.
# 현재 설정(config.yaml)에서는 S3 관련 JAR를 --packages로 지정하고 있습니다.
ENV SPARK_HOME=/opt/spark

USER spark
