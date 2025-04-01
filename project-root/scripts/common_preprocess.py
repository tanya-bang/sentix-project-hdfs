# scripts/common_preprocess.py
import re
import pandas as pd
from infra.util import load_keyword_file

# 정치 키워드, URL 포함 여부 체크 함수
def contains_political(text: str, keywords: list[str]) -> bool:
    text = re.sub(r'\s+', '', str(text))
    return any(keyword in text for keyword in keywords)

def contains_url(text: str) -> bool:
    return bool(re.search(r'https?://|www\.|\.com|\.kr', str(text)))

# 텍스트 정제 함수 (TF-IDF용)
def clean_for_tfidf(text: str) -> str:
    text = text.replace('\n', ' ').replace('"', "'").replace(',', '')  # 줄바꿈/따옴표/쉼표 제거
    text = re.sub(r'<[^>]+>', '', text)  # HTML 태그 제거
    text = re.sub(r'https?://\S+|www\.\S+', '', text)  # URL 제거
    text = re.sub(r'[^가-힣a-zA-Z0-9\s]', '', text)  # 특수문자 제거
    return re.sub(r'\s+', ' ', text).strip()  # 공백 정리

# 텍스트 정제 함수 (BERT용, 감성 이모티콘 허용)
def clean_for_bert(text: str) -> str:
    text = text.replace('\n', ' ').replace('"', "'").replace(',', '')  # 줄바꿈/따옴표/쉼표 제거
    text = re.sub(r'<[^>]+>', '', text)  # HTML 태그 제거
    text = re.sub(r'https?://\S+|www\.\S+', '', text)  # URL 제거
    text = re.sub(r'[^\u3131-\u3163\uAC00-\uD7A3a-zA-Z0-9\s!?.,]', '', text)  # 자모 포함 허용
    return re.sub(r'\s+', ' ', text).strip()  # 공백 정리

# 전처리 메인 함수
def preprocess_comments(df: pd.DataFrame, political_path: str, text: str, timestamp: str) -> pd.DataFrame:
    df = df.copy()
    political_keywords = load_keyword_file(political_path)

    def process_row(row):
        text_val = row[text]
        time_val = row[timestamp]
        if pd.isna(text_val) or pd.isna(time_val):
            return None
        if contains_political(text_val, political_keywords) or contains_url(text_val):
            return None  # 정치 키워드 또는 URL 포함 시 삭제
        tfidf = clean_for_tfidf(text_val)
        bert = clean_for_bert(text_val)
        if not bert or len(bert.strip()) < 2:
            return None  # BERT 텍스트가 비어있거나 2자 미만이면 삭제
        return pd.Series({
            "raw_text": str(text_val).replace('\n', ' ').replace('"', "'").replace(',', ''),
            "text_tfidf": tfidf,
            "text_bert": bert,
            "time": time_val
        })

    processed = df[[text, timestamp]].apply(process_row, axis=1)
    processed = processed.dropna().copy()
    processed["time_floor_5s"] = pd.to_datetime(processed["time"]).dt.floor("5s")
    processed = processed.drop_duplicates(subset=["raw_text", "time_floor_5s"])
    return processed.drop(columns=["time_floor_5s"]).reset_index(drop=True)
