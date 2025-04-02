
from pathlib import Path
import pandas as pd
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from infra.hdfs_client import get_client
from infra.util import get_stock_name, load_csv_from_hdfs, save_csv_to_hdfs
from io import StringIO

stock_name = get_stock_name()
#stock_name='apple'

# HDFS 클라이언트 생성
client = get_client()

# ✅ 경로 구성
model_path = Path(f"/root/project-root/models/kcbert_3class_apple")  # 로컬 모델 경로
input_path = f"/project-root/data/_1_preprocess/{stock_name}_filtered.csv"    #원본
output_path = f"/project-root/data/_3_predict/{stock_name}_predict_bert.csv"  #원본
#input_path = f"/project-root/data/_1_preprocess/{stock_name}_filtered_airflowtest.csv"  #airflow test용 경로
#output_path = f"/project-root/data/_2_labeling/airflowtest/{stock_name}_predict_bert.csv" #airflow test용 경로

# ✅ 모델 및 토크나이저 로드
tokenizer = AutoTokenizer.from_pretrained(model_path, local_files_only=True)
model = AutoModelForSequenceClassification.from_pretrained(model_path, local_files_only=True)
model.eval()

# HDFS에서 데이터 로드
df = load_csv_from_hdfs(client, input_path)
# text_bert 저장 시 CSV 필드 깨짐 방지 (쉼표·줄바꿈 제거)
df["text_bert"] = df["text_bert"].astype(str).str.replace(",", " ", regex=False).str.replace("\n", " ", regex=False)

if len(df) >= 1000:
    df = df.sample(n=1000, random_state=41).reset_index(drop=True)
else:
    df = df.sample(frac=1.0, random_state=41).reset_index(drop=True)  # 전부 섞어서 사용

texts = df["text_bert"].fillna("").tolist()

# ✅ 배치 추론
results = []
batch_size = 32

for i in range(0, len(texts), batch_size):
    batch_texts = texts[i:i+batch_size]
    inputs = tokenizer(batch_texts, return_tensors="pt", padding=True, truncation=True, max_length=128)

    with torch.no_grad():
        outputs = model(**inputs)
        probs = torch.softmax(outputs.logits, dim=-1)

    for prob in probs:
        prob = prob.tolist()
        label = int(torch.argmax(torch.tensor(prob)))
        results.append({
            "prob_fear": round(prob[0], 4),
            "prob_neutral": round(prob[1], 4),
            "prob_greed": round(prob[2], 4),
            "pred_label": label
        })

# ✅ 결과 DataFrame 생성
predict_df = pd.DataFrame(results)
final_df = pd.concat([df.reset_index(drop=True), predict_df], axis=1)

# ✅ HDFS에 저장
save_csv_to_hdfs(df, output_path, client)
print(f"✅ 추론 결과 HDFS에 저장 완료: {output_path}")
