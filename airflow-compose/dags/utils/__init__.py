#!/usr/bin/env python3
"""
Airflow 공통 유틸리티 함수들
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any


def get_process_date_from_context(context: Dict[str, Any]) -> str:
    """
    Airflow context에서 처리할 날짜를 추출
    
    Args:
        context: Airflow task context
        
    Returns:
        str: 처리할 날짜 (YYYY-MM-DD)
    """
    execution_date = context['execution_date']
    process_date = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    return process_date


def setup_logging(name: str) -> logging.Logger:
    """
    로깅 설정
    
    Args:
        name: 로거 이름
        
    Returns:
        logging.Logger: 설정된 로거
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(name)


def validate_date_format(date_string: str) -> bool:
    """
    날짜 형식 검증
    
    Args:
        date_string: 검증할 날짜 문자열
        
    Returns:
        bool: 유효한 형식이면 True
    """
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False


def get_s3_path(bucket: str, prefix: str, date: str) -> str:
    """
    S3 경로 생성
    
    Args:
        bucket: S3 버킷 이름
        prefix: 경로 접두사
        date: 날짜 (YYYY-MM-DD)
        
    Returns:
        str: 완전한 S3 경로
    """
    return f"s3a://{bucket}/{prefix}/{date}/"
