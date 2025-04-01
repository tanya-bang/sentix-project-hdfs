import pandas as pd
from infra.hdfs_client import get_client

stock_name = "apple"
hdfs_path = f"/project-root/data/_1_preprocess/{stock_name}_filtered.csv"

client = get_client()

with client.read(hdfs_path, encoding="utf-8-sig") as reader:
    df = pd.read_csv(reader, encoding="utf-8-sig")

print(f"✅ 총 {len(df):,}개 행 로드됨")
print(df.head())
