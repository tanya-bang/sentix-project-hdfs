import os
from infra.hdfs_client import get_client
from infra.util import load_csv_from_hdfs, save_csv_to_hdfs
from scripts.common_labeling import weak_label_by_count, split_train_valid

stock_name = os.environ.get("STOCK_NAME")
if not stock_name:
    raise ValueError("환경변수 STOCK_NAME이 설정되지 않았습니다.")

# 경로 설정
input_path = f"/project-root/data/_1_preprocess/{stock_name}_filtered.csv"
output_dir = f"/project-root/data/_2_labeling"
labeled_path = f"{output_dir}/{stock_name}_labeled_3class.csv"
train_path = f"{output_dir}/{stock_name}_train_3class.csv"
valid_path = f"{output_dir}/{stock_name}_valid_3class.csv"

# HDFS 클라이언트 생성
client = get_client()

# HDFS → Pandas
df = load_csv_from_hdfs(client, input_path)

# 라벨링 (text_bert 기준)
df["label"] = df["text_bert"].apply(lambda x: weak_label_by_count(str(x)))

# 전체 저장
save_csv_to_hdfs(df, labeled_path, client)
print(f"✅ {stock_name} 라벨링 완료! 총 {len(df):,}개 저장")
print("✅ 전체 라벨 분포 (0=공포, 1=중립, 2=탐욕):")
print(df["label"].value_counts().sort_index().rename_axis("label"))

# train/valid 분할
df_train, df_valid = split_train_valid(df)

# 저장
save_csv_to_hdfs(df_train, train_path, client)
save_csv_to_hdfs(df_valid, valid_path, client)

print(f"✅ {stock_name} 라벨링 완료! (train: {len(df_train):,}, valid: {len(df_valid):,}, 합계: {len(df_train) + len(df_valid):,})")
