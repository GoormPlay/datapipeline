import streamlit as st
import pandas as pd
import requests
import os

# API 기본 URL 설정 (FastAPI 서버 주소)
# 환경 변수가 없으면 http://localhost:8000를 기본값으로 사용
API_BASE_URL = os.getenv("API_URL", "http://localhost:8000")

# --- API 호출 함수 ---

@st.cache_data(ttl=600) # API 응답을 10분 동안 캐싱하여 불필요한 호출 방지
def get_databases():
    """/databases 엔드포인트를 호출하여 데이터베이스 목록을 가져옵니다."""
    try:
        response = requests.get(f"{API_BASE_URL}/databases")
        response.raise_for_status() # 200번대 코드가 아니면 예외 발생
        data = response.json()
        if "databases" in data:
            return data["databases"]
        else:
            st.error(f"API에서 데이터베이스 목록을 가져오는 데 실패했습니다: {data.get('error', '알 수 없는 오류')}")
            return []
    except requests.exceptions.RequestException as e:
        st.error(f"API 서버({API_BASE_URL})에 연결할 수 없습니다: {e}")
        return []

@st.cache_data(ttl=600)
def get_tables(database: str):
    """/tables 엔드포인트를 호출하여 특정 데이터베이스의 테이블 목록을 가져옵니다."""
    if not database:
        return []
    try:
        response = requests.get(f"{API_BASE_URL}/tables", params={"database": database})
        response.raise_for_status()
        data = response.json()
        if "tables" in data:
            return data["tables"]
        else:
            st.error(f"'{database}'의 테이블 목록을 가져오는 데 실패했습니다: {data.get('error', '알 수 없는 오류')}")
            return []
    except requests.exceptions.RequestException as e:
        st.error(f"API 서버에 연결할 수 없습니다: {e}")
        return []

@st.cache_data(ttl=300) # 데이터는 5분 동안 캐싱
def get_table_data(database: str, table: str, limit: int = 100):
    """/table-data 엔드포인트를 호출하여 테이블 데이터를 가져옵니다."""
    if not database or not table:
        return None
    try:
        params = {"database": database, "table": table, "limit": limit}
        response = requests.get(f"{API_BASE_URL}/table-data", params=params)
        response.raise_for_status()
        data = response.json()
        if "data" in data:
            return pd.DataFrame(data["data"])
        else:
            st.error(f"'{database}.{table}'의 데이터를 가져오는 데 실패했습니다: {data.get('error', '알 수 없는 오류')}")
            return None
    except requests.exceptions.RequestException as e:
        st.error(f"API 서버에 연결할 수 없습니다: {e}")
        return None

@st.cache_data(ttl=60) # 쿼리 결과는 1분만 캐싱
def run_query(query: str):
    """/run-query 엔드포인트를 호출하여 사용자 정의 쿼리를 실행합니다."""
    if not query:
        return None, None, "쿼리가 비어있습니다."
    try:
        # API는 GET 요청이므로 쿼리를 URL 파라미터로 전달
        params = {"query": query}
        response = requests.get(f"{API_BASE_URL}/run-query", params=params)
        response.raise_for_status()
        data = response.json()

        executed_query = data.get("query") # API가 수정한 쿼리를 가져옴

        if "data" in data:
            return pd.DataFrame(data["data"]), executed_query, None # df, executed_query, error
        else:
            error_message = data.get('error', '알 수 없는 오류')
            return None, executed_query, error_message
    except requests.exceptions.RequestException as e:
        return None, query, f"API 서버 연결 오류: {e}" # df, executed_query, error
    except Exception as e:
        return None, query, f"알 수 없는 오류 발생: {e}"

# --- Streamlit UI 구성 ---

st.set_page_config(layout="wide", page_title="Iceberg 데이터 시각화 대시보드")

st.title("🧊 Iceberg 데이터 탐색기")
st.write("FastAPI를 통해 Iceberg 테이블의 데이터를 조회하고 시각화하는 대시보드입니다.")

# 사이드바: 데이터베이스 및 테이블 선택
st.sidebar.header("데이터 선택")
databases = get_databases()
if not databases:
    st.warning("조회할 데이터베이스가 없거나 API 서버에 연결할 수 없습니다. FastAPI 서버가 실행 중인지 확인하세요.")
else:
    selected_db = st.sidebar.selectbox("1. 데이터베이스 선택", options=databases)

    if selected_db:
        tables = get_tables(selected_db)
        if not tables:
            st.info(f"'{selected_db}' 데이터베이스에 테이블이 없습니다.")
        else:
            selected_table = st.sidebar.selectbox("2. 테이블 선택", options=tables)
            limit = st.sidebar.slider("가져올 데이터 수", 10, 1000, 100)

            if st.sidebar.button("데이터 불러오기", type="primary"):
                # 사용자가 선택한 값을 session_state에 저장하여 앱이 재실행되어도 유지
                st.session_state.selected_db = selected_db
                st.session_state.selected_table = selected_table
                st.session_state.limit = limit

# 메인 화면: 데이터 표시 및 시각화
if 'selected_table' in st.session_state and st.session_state.selected_table:
    db = st.session_state.selected_db
    table = st.session_state.selected_table
    limit = st.session_state.limit

    st.header(f"'{db}.{table}' 데이터 미리보기 (최대 {limit}개)")

    with st.spinner("데이터를 불러오는 중입니다..."):
        df = get_table_data(db, table, limit)

    if df is not None and not df.empty:
        st.dataframe(df)

        # video_clicks_summary 테이블에 대한 특별 시각화
        if table == "video_clicks_summary":
            st.header("🎬 인기 비디오 TOP 10")
            if "title" in df.columns and "click_count" in df.columns:
                # click_count 기준으로 내림차순 정렬 후 상위 10개 선택
                top_10_videos = df.sort_values(by="click_count", ascending=False).head(10)
                
                if not top_10_videos.empty:
                    # Streamlit의 bar_chart는 x, y 인자를 직접 받을 수 있습니다.
                    st.bar_chart(top_10_videos, x="title", y="click_count")
                    st.write("가장 많이 클릭된 비디오:")
                    st.dataframe(top_10_videos.set_index("title"))
                else:
                    st.info("인기 비디오를 시각화할 데이터가 충분하지 않습니다.")
            else:
                st.warning("`video_clicks_summary` 테이블에 'title' 또는 'click_count' 컬럼이 없습니다.")

        st.header("📊 자동 시각화")
        st.write("데이터 타입에 따라 기본적인 시각화를 제공합니다.")

        # 시각화할 컬럼 선택
        vis_cols = st.multiselect("시각화할 컬럼을 선택하세요.", options=df.columns)

        if vis_cols:
            for col in vis_cols:
                with st.container(border=True):
                    st.subheader(f"`{col}` 컬럼 분석")
                    # 숫자형 데이터 처리
                    if pd.api.types.is_numeric_dtype(df[col]):
                        st.write("**요약 통계:**")
                        st.write(df[col].describe())
                        
                        st.write("**값 분포 (막대 차트):**")
                        # NaN 값을 제외하고 시각화
                        chart_data = df[col].dropna().value_counts()
                        if not chart_data.empty:
                            st.bar_chart(chart_data)
                        else:
                            st.info("데이터가 없어 차트를 그릴 수 없습니다.")
                    
                    # 카테고리형(Object) 데이터 처리
                    else:
                        st.write("**값 별 개수:**")
                        value_counts = df[col].value_counts()
                        st.bar_chart(value_counts)
                        st.write(value_counts)

    elif df is not None and df.empty:
        st.info("테이블에 데이터가 없습니다.")
    else:
        st.error("데이터를 불러오지 못했습니다. API 서버 로그를 확인해주세요.")

else:
    st.info("왼쪽 사이드바에서 데이터베이스와 테이블을 선택하고 '데이터 불러오기' 버튼을 클릭하세요.")