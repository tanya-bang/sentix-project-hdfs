import pandas as pd
from sklearn.model_selection import train_test_split
from infra.util import load_keyword_file

# 키워드 파일 내장 로드 함수 (경로 고정)
def load_keywords() -> tuple[set[str], set[str]]:
    fear = load_keyword_file("/project-root/data/_0_raw/fear_keywords.txt")
    greed = load_keyword_file("/project-root/data/_0_raw/greed_keywords.txt")
    return set(fear), set(greed)

# 약한 라벨링: 공포/탐욕 키워드 수 비교
def weak_label_by_count(text: str) -> int:
    if not hasattr(weak_label_by_count, "_keywords"):
        weak_label_by_count._keywords = load_keywords()
    fear_keywords, greed_keywords = weak_label_by_count._keywords

    fear_count = sum(1 for word in fear_keywords if word in text)
    greed_count = sum(1 for word in greed_keywords if word in text)

    if fear_count > greed_count:
        return 0  # 공포
    elif greed_count > fear_count:
        return 2  # 탐욕
    else:
        return 1  # 중립

# train/valid 분할 함수
def split_train_valid(df: pd.DataFrame, test_size: float = 0.2, seed: int = 42) -> tuple[pd.DataFrame, pd.DataFrame]:
    return train_test_split(df, test_size=test_size, stratify=df["label"], random_state=seed)
