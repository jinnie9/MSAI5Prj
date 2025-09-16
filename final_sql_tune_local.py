import os
from dotenv import load_dotenv
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import threading, glob, json, requests
from azure.storage.blob import BlobServiceClient
from openai import AzureOpenAI
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

# -----------------------
# 설정
# -----------------------
load_dotenv()
BLOB_CONN_STR = os.getenv("BLOB_CONN_STR")
CONTAINER = "msaiquery"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")
AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")

AUTO_FETCH_INTERVAL = 300
CACHE_TTL = 600

# -----------------------
# Blob Storage 초기화
# -----------------------
@st.cache_resource
def get_blob_container():
    try:
        blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
        return blob_service.get_container_client(CONTAINER)
    except:
        return None

container_client = get_blob_container()

# -----------------------
# OpenAI 초기화
# -----------------------
@st.cache_resource
def get_openai_client():
    try:
        client = AzureOpenAI(
            api_key=OPENAI_API_KEY,
            api_version=OPENAI_API_VERSION,
            azure_endpoint=AZURE_ENDPOINT
        )
        return client
    except:
        return None

client = get_openai_client()

# -----------------------
# SQL 튜닝
# -----------------------
def tune_sql_with_openai(sql_text: str):
    if not client:
        return "OpenAI 클라이언트 초기화 필요"
    if not sql_text or sql_text.strip() == "" or sql_text == "SQL 정보 없음":
        return "튜닝할 SQL 없음"
    prompt = f"SQL 최적화: {sql_text}"
    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=1000
        )
        return resp.choices[0].message.content
    except Exception as e:
        return f"튜닝 오류: {e}"

# -----------------------
# 로컬 JSON 로드
# -----------------------
@st.cache_data(ttl=CACHE_TTL)
def load_data_local(folder="data"):
    records = []
    for file in glob.glob(f"{folder}/*.json"):
        try:
            with open(file,'r',encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict):
                data = [data]
            for r in data:
                r.setdefault("timestamp", datetime.now().isoformat())
                r.setdefault("app_name", "unknown")
                r.setdefault("elapse_time", 0.0)
                r.setdefault("user_ip", "0.0.0.0")
                r.setdefault("cfg_path", "/config/default")
                r.setdefault("sql", None)
                records.append(r)
        except:
            continue
    if records:
        df = pd.DataFrame(records)
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors='coerce').fillna(datetime.now())
        df["elapse_time"] = pd.to_numeric(df["elapse_time"], errors='coerce').fillna(0.0)
        df["sql"] = df["sql"].fillna("SQL 정보 없음")
        return df
    else:
        return pd.DataFrame(columns=["timestamp","app_name","elapse_time","user_ip","cfg_path","sql"])

# -----------------------
# Blob 데이터 로드
# -----------------------
@st.cache_data(ttl=CACHE_TTL)
def load_data_blob(start, end):
    if not container_client:
        return pd.DataFrame(columns=["timestamp","app_name","elapse_time","user_ip","cfg_path","sql"])
    records = []
    for blob in container_client.list_blobs():
        try:
            data_bytes = container_client.download_blob(blob.name).readall()
            data = json.loads(data_bytes)
            if isinstance(data, dict):
                data = [data]
            for r in data:
                r.setdefault("timestamp", datetime.now().isoformat())
                r.setdefault("app_name", "unknown")
                r.setdefault("elapse_time", 0.0)
                r.setdefault("user_ip", "0.0.0.0")
                r.setdefault("cfg_path", "/config/default")
                r.setdefault("sql", None)
                records.append(r)
        except:
            continue
    if records:
        df = pd.DataFrame(records)
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors='coerce')
        df = df[(df["timestamp"] >= start) & (df["timestamp"] <= end)]
        df["elapse_time"] = pd.to_numeric(df["elapse_time"], errors='coerce').fillna(0.0)
        df["sql"] = df["sql"].fillna("SQL 정보 없음")
        return df
    return pd.DataFrame(columns=["timestamp","app_name","elapse_time","user_ip","cfg_path","sql"])

# -----------------------
# URL 호출 -> Blob 저장
# -----------------------
def fetch_json_to_blob(url):
    if not container_client:
        st.error("Blob Storage 필요")
        return
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        now = datetime.utcnow()
        blob_name = f"logs/{now.strftime('%Y/%m/%d/%H/%M')}.json"
        container_client.upload_blob(blob_name, json.dumps(data), overwrite=True)
    except Exception as e:
        st.error(f"URL fetch 오류: {e}")

def run_periodic_fetch(url, interval_sec=AUTO_FETCH_INTERVAL):
    while True:
        fetch_json_to_blob(url)
        threading.Event().wait(interval_sec)

# -----------------------
# Streamlit UI
# -----------------------
st.title("Elapse Time 모니터링 & SQL 튜닝")

# 자동 새로고침
refresh_interval = st.number_input("페이지 자동 새로고침(초)", min_value=30, max_value=3600, value=600)
st.components.v1.html(f"<meta http-equiv='refresh' content='{refresh_interval}'>", height=0)

# 데이터 소스 선택
mode = st.radio("데이터 소스", ["로컬 JSON", "Blob Storage"])

# 운영 URL 수집
if mode=="Blob Storage":
    url_input = st.text_input("API URL")
    if st.button("주기적 수집 시작") and url_input:
        thread = threading.Thread(target=run_periodic_fetch, args=(url_input,), daemon=True)
        thread.start()
        st.success(f"주기적 수집 시작 ({AUTO_FETCH_INTERVAL}초 간격)")

# 날짜 범위
col1, col2 = st.columns(2)
start_date = col1.date_input("시작일", datetime.now()-timedelta(days=1))
end_date = col2.date_input("종료일", datetime.now())
start_dt = datetime.combine(start_date, datetime.min.time())
end_dt = datetime.combine(end_date, datetime.max.time())

# 데이터 로드
df = load_data_local() if mode=="로컬 JSON" else load_data_blob(start_dt,end_dt)
df = df[(df["timestamp"] >= start_dt) & (df["timestamp"] <= end_dt)]
st.info(f"총 {len(df)}개 레코드 로드")

if df.empty:
    st.warning("표시할 데이터 없음")
    st.stop()

# cfg_path 필터
cfg_options = df["cfg_path"].dropna().unique().tolist()
cfg_filter = st.multiselect("cfg_path 필터", cfg_options)
if cfg_filter:
    df = df[df["cfg_path"].isin(cfg_filter)]

# 시간대/ cfg_path 평균 그래프
avg_df = df.groupby([pd.Grouper(key="timestamp", freq="10min"), "cfg_path"])["elapse_time"].mean().reset_index()
if not avg_df.empty:
    st.subheader("시간대 / cfg_path별 평균 Elapse Time")
    st.line_chart(avg_df, x="timestamp", y="elapse_time", color="cfg_path")

# Top10
top10_display = df.nlargest(10, "elapse_time").copy()
top10_display["sql"] = top10_display["sql"].fillna("SQL 정보 없음")

st.subheader("Top10 Elapse Time")
gb = GridOptionsBuilder.from_dataframe(top10_display)
gb.configure_selection("single", use_checkbox=False)
gb.configure_column("sql", wrapText=True, autoHeight=True, width=400)
grid_options = gb.build()
grid_response = AgGrid(top10_display, gridOptions=grid_options, update_mode=GridUpdateMode.SELECTION_CHANGED, height=300)

# session_state 초기화
if "selected_sql" not in st.session_state:
    st.session_state.selected_sql = None

# 선택 행 처리
selected_rows = grid_response.get("selected_rows", [])
if selected_rows is not None and selected_rows.shape[0] > 0:
    # print(selected_rows.head())
    # row_data = selected_rows[0]
    target_row = selected_rows.iloc[0]
    st.session_state.selected_sql = target_row.get("sql", "SQL 정보 없음")
    # st.session_state.selected_sql = row_data.get("sql", "SQL 정보 없음")

# SQL 표시 + 튜닝 버튼
st.subheader("선택한 SQL")
st.code(st.session_state.selected_sql or "SQL 정보 없음", language="sql")
if st.button("🚀 AI로 SQL 튜닝하기"):
    with st.spinner("응답을 기다리는 중..."):
        if st.session_state.selected_sql and st.session_state.selected_sql != "SQL 정보 없음":
            advice = tune_sql_with_openai(st.session_state.selected_sql)
            st.markdown("### 🔍 AI SQL 튜닝 결과")
            st.code(advice, language="sql")
        else:
            st.warning("튜닝할 SQL 없음")
