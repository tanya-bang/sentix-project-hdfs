import pandas as pd
import numpy as np
from infra.hdfs_client import get_client
from infra.util import load_csv_from_hdfs
import os

# ✅ 종목명과 기준 날짜 설정
# stock_name = "samsung"    
stock_name = os.environ.get("STOCK_NAME")
if not stock_name:
    raise ValueError("환경변수 STOCK_NAME이 설정되지 않았습니다.")     
ref_date_str = "2025-03-20"    # 기준 날짜 (KST 기준)
ref_date = pd.to_datetime(ref_date_str).tz_localize("Asia/Seoul")

# HDFS 경로 설정 및 로드
input_path = f'/project-root/data/_3_predict/{stock_name}_predict_bert.csv'
client = get_client()
df = load_csv_from_hdfs(client, input_path)

# ✅ 가중치 계산 함수
def compute_time_weight(df: pd.DataFrame, tau: int = 86400) -> pd.DataFrame:
    df["time"] = pd.to_datetime(df["time"], errors='coerce')
    df = df.dropna(subset=["time"])
    df["time"] = df["time"].dt.tz_convert("Asia/Seoul")
    latest_time = df["time"].max()
    df["delta_seconds"] = (latest_time - df["time"]).dt.total_seconds()
    df["weight"] = np.exp(-df["delta_seconds"] / tau)
    return df

# ✅ 시간 정렬 및 기준 이전 데이터 샘플링
df["time"] = pd.to_datetime(df["time"], errors='coerce').dt.tz_convert("Asia/Seoul")
df = df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)

sub_df = df[df["time"] < ref_date].sort_values("time", ascending=False).head(100).copy()
sub_df = sub_df.sort_values("time").reset_index(drop=True)

# ✅ 감성 점수 계산
if len(sub_df) < 100:
    print(f"⚠️ {ref_date.date()} 기준 {stock_name} 데이터가 100개 미만이므로 계산하지 않음")
else:
    sub_df = compute_time_weight(sub_df)

    sub_df['individual_score'] = (
        sub_df['prob_fear'] * 0 +
        sub_df['prob_neutral'] * 50 +
        sub_df['prob_greed'] * 100
    )

    sub_df['weighted_score'] = sub_df['individual_score'] * sub_df['weight']

    total_weighted_score = sub_df['weighted_score'].sum()
    total_weight = sub_df['weight'].sum()
    final_score = total_weighted_score / total_weight if total_weight > 0 else 0

    print(f"📅 기준 날짜: {ref_date.date()} (KST)")
    print(f"🧪 샘플링 수: {stock_name} : {len(sub_df)}개")
    print(f"📊 커뮤니티 감성 지수 (공포/탐욕 점수): {final_score:.2f}")

print("🎉 작업 완료.")