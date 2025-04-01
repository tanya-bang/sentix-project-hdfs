import os
from infra.hdfs_client import get_client
from infra.util import load_csv_from_hdfs, save_csv_to_hdfs
from scripts.common_preprocess import preprocess_comments

stock_name = os.environ.get("STOCK_NAME")
if not stock_name:
    raise ValueError("환경변수 STOCK_NAME이 설정되지 않았습니다.")
#stock_name='apple'

# HDFS 경로 설정
input_path = f"/project-root/data/_0_raw/{stock_name}_comments.csv"
output_path = f"/project-root/data/_1_preprocess/{stock_name}_filtered.csv"

# HDFS 클라이언트 생성
client = get_client()

# CSV 로드
df = load_csv_from_hdfs(client, input_path)

# 전처리 수행
filtered_df = preprocess_comments(
    df,
    political_path="/project-root/data/_0_raw/political_keywords.txt",
    text="Message",
    timestamp="Updated At"
)

# HDFS에 저장 (CSV 포맷 깨짐 방지)
save_csv_to_hdfs(filtered_df, output_path, client)

print(f"✅ {stock_name} 전처리 완료! 남은 댓글 수: {len(filtered_df):,}")