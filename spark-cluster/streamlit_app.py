import streamlit as st
import pandas as pd
import requests
import os
import plotly.express as px

# API 기본 URL 설정 (FastAPI 서버 주소)
API_BASE_URL = os.getenv("API_URL", "http://localhost:8000")

# --- Streamlit UI 구성 ---
st.set_page_config(layout="wide", page_title="사용자 행동 분석 대시보드", page_icon="📈")

st.title("📈 사용자 행동 분석 대시보드")
st.markdown("FastAPI를 통해 실시간으로 집계된 Iceberg 테이블 데이터를 조회합니다.")

# --- API 호출 함수 ---
@st.cache_data(ttl=60) # API 응답을 1분 동안 캐싱
def fetch_analytics_data(endpoint: str, limit: int):
    """분석 API 엔드포인트에서 데이터를 가져옵니다."""
    try:
        url = f"{API_BASE_URL}/{endpoint}" # 전체 경로를 받도록 수정
        params = {"limit": limit} # limit은 공통 파라미터로 사용
        response = requests.get(url, params=params)
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except requests.exceptions.RequestException as e:
        st.error(f"API 서버({url}) 호출 중 오류 발생: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"데이터 처리 중 오류 발생: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def fetch_recent_data(endpoint: str, minutes: int, limit: int):
    """최근 데이터 조회 API를 호출합니다."""
    try:
        url = f"{API_BASE_URL}/{endpoint}"
        params = {"minutes_ago": minutes, "limit": limit}
        response = requests.get(url, params=params)
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except requests.exceptions.RequestException as e:
        st.error(f"API 서버({url}) 호출 중 오류 발생: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"데이터 처리 중 오류 발생: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def fetch_raw_log_data(limit: int, where_clause: str = ""):
    """/table-data 엔드포인트에서 user_logs 데이터를 가져옵니다."""
    try:
        url = f"{API_BASE_URL}/table-data"
        # API 파라미터에 빈 where_clause는 보내지 않도록 처리
        params = {
            "database": "analytics",
            "table": "user_logs",
            "limit": limit,
        }
        if where_clause:
            params["where_clause"] = where_clause

        response = requests.get(url, params=params)
        response.raise_for_status()
        response_data = response.json()
        if "data" in response_data:
            return pd.DataFrame(response_data["data"])
        else:
            st.error(f"API 응답에 'data' 키가 없습니다: {response_data.get('error', '알 수 없는 오류')}")
            return pd.DataFrame()
    except requests.exceptions.RequestException as e:
        st.error(f"API 서버({url}) 호출 중 오류 발생: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"데이터 처리 중 오류 발생: {e}")
        return pd.DataFrame()

# 사이드바
with st.sidebar:
    st.header("⚙️ 컨트롤 패널")
    limit = st.slider("상위 N개 항목 조회", min_value=5, max_value=50, value=10, step=5)
    if st.button("데이터 새로고침", type="primary"):
        st.cache_data.clear()
        st.rerun()

# 탭 생성
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "🕒 실시간 활동",
    "📈 사용자 관심도", 
    "▶️ 인기 재생", 
    "🖱️ 인기 추천",
    "💥 종합 클릭 (기존)",
    "📜 원본 로그 조회"
])

# 1. 실시간 활동 탭 (신규)
with tab1:
    st.header("🕒 실시간 활동 (최근 30분)")
    st.markdown("최근 30분 동안 집계된 사용자 관심도 순위입니다. 현재 가장 '핫'한 콘텐츠를 보여줍니다.")
    with st.spinner("최근 활동 데이터 로딩 중..."):
        df_recent = fetch_recent_data("analytics/recent-user-interest", minutes=30, limit=limit)
    
    if not df_recent.empty:
        fig = px.bar(df_recent, 
                     x="recent_score", 
                     y="title", 
                     orientation='h', 
                     title=f"최근 30분간 사용자 관심도 TOP {limit}", 
                     labels={"recent_score": "최근 관심도 점수", "title": "콘텐츠 제목"},
                     text='recent_score')
        fig.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
        
        with st.expander("상세 데이터 보기"):
            st.dataframe(df_recent)
    else:
        st.info("최근 30분 내에 집계된 데이터가 없습니다.")

# 2. 사용자 관심도 탭
with tab2:
    st.header("📈 사용자 관심도 높은 콘텐츠")
    st.markdown("좋아요, 리뷰, 평점, 재생 시작 등을 종합하여 **사용자별 관심도 점수**가 가장 높은 콘텐츠입니다.")
    with st.spinner("데이터 로딩 중..."):
        df_interest = fetch_analytics_data("analytics/user-interest", limit)
    
    if not df_interest.empty:
        # Plotly로 시각화 개선
        fig = px.bar(df_interest, 
                     x="total_score", 
                     y="title", 
                     orientation='h', 
                     title=f"상위 {limit}개 콘텐츠의 사용자 관심도 점수", 
                     labels={"total_score": "총 관심도 점수", "title": "콘텐츠 제목"},
                     text='total_score')
        fig.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
        
        with st.expander("상세 데이터 보기"):
            st.dataframe(df_interest)
    else:
        st.warning("표시할 사용자 관심도 데이터가 없습니다. Spark 스트리밍 작업이 실행 중인지 확인해주세요.")

# 3. 가장 많이 재생된 콘텐츠 탭
with tab3:
    st.header("▶️ 가장 많이 재생된 콘텐츠")
    st.markdown("사용자들이 **재생 시작(play_start)** 버튼을 가장 많이 누른 콘텐츠입니다.")
    with st.spinner("데이터 로딩 중..."):
        df_played = fetch_analytics_data("analytics/top-played", limit)
    
    if not df_played.empty:
        fig = px.bar(df_played, 
                     x="total_plays", 
                     y="title", 
                     orientation='h', 
                     title=f"상위 {limit}개 콘텐츠의 재생 시작 횟수", 
                     labels={"total_plays": "총 재생 시작 횟수", "title": "콘텐츠 제목"},
                     text='total_plays')
        fig.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
        
        with st.expander("상세 데이터 보기"):
            st.dataframe(df_played)
    else:
        st.warning("표시할 재생 데이터가 없습니다.")

# 4. 추천 클릭이 많은 콘텐츠 탭
with tab4:
    st.header("🖱️ 추천 클릭이 많은 콘텐츠")
    st.markdown("추천 목록을 통해 사용자들이 가장 많이 **클릭**한 콘텐츠입니다.")
    with st.spinner("데이터 로딩 중..."):
        df_recom = fetch_analytics_data("analytics/top-recommended-clicks", limit)
    
    if not df_recom.empty:
        fig = px.bar(df_recom, 
                     x="total_recom_clicks", 
                     y="title", 
                     orientation='h', 
                     title=f"상위 {limit}개 콘텐츠의 추천 클릭 수", 
                     labels={"total_recom_clicks": "총 추천 클릭 수", "title": "콘텐츠 제목"},
                     text='total_recom_clicks')
        fig.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
        
        with st.expander("상세 데이터 보기"):
            st.dataframe(df_recom)
    else:
        st.warning("표시할 추천 클릭 데이터가 없습니다.")

# 5. 종합 클릭이 많은 콘텐츠 탭 (기존)
with tab5:
    st.header("💥 종합 클릭이 많은 콘텐츠 (기존)")
    st.markdown("단순 **콘텐츠 클릭(content_click)** 횟수가 가장 많은 콘텐츠입니다.")
    with st.spinner("데이터 로딩 중..."):
        df_clicks = fetch_analytics_data("analytics/top-clicks", limit)
    
    if not df_clicks.empty:
        fig = px.bar(df_clicks, 
                     x="total_clicks", 
                     y="title", 
                     orientation='h', 
                     title=f"상위 {limit}개 콘텐츠의 종합 클릭 수", 
                     labels={"total_clicks": "총 클릭 수", "title": "콘텐츠 제목"},
                     text='total_clicks')
        fig.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
        
        with st.expander("상세 데이터 보기"):
            st.dataframe(df_clicks)
    else:
        st.warning("표시할 종합 클릭 데이터가 없습니다.")

# 6. 원본 로그 데이터 조회 탭
with tab6:
    st.header("📜 원본 로그 데이터 조회 (`user_logs`)")
    st.markdown("가공되지 않은 원본 `user_logs` 테이블의 데이터를 직접 조회합니다. 데이터 양이 많을 수 있으므로 필터 사용을 권장합니다.")

    with st.form("log_filter_form"):
        st.write("##### 🔍 데이터 필터링")
        where_input = st.text_input(
            "WHERE 절 입력",
            placeholder="예: eventType = 'like_click' AND rating > 3"
        )
        submitted = st.form_submit_button("로그 데이터 조회")

    if submitted:
        with st.spinner("원본 로그 데이터 로딩 중..."):
            df_logs = fetch_raw_log_data(limit, where_input)
        
        if not df_logs.empty:
            st.success(f"**{len(df_logs)}개**의 로그 데이터를 조회했습니다.")
            st.dataframe(df_logs)
        else:
            st.warning("조건에 맞는 로그 데이터가 없거나 조회에 실패했습니다.")