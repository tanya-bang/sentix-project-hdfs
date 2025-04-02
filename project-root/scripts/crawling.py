import time
import requests
import pandas as pd
from datetime import datetime
from io import BytesIO
from infra.hdfs_client import get_client
from infra.util import get_stock_name, cal_std_day

# ✅ 종목 코드 매핑
STOCK_IDS = {
    "samsung": "KR7005930003",
    "skhynix": "KR7000660001",
    "apple": "US19801212001",
    "nvidia": "US19990122001"
}

# ✅ Toss API 요청 헤더 구성
def get_headers():
    return {
        "accept": "application/json",
        "content-type": "application/json",
        "origin": "https://tossinvest.com",
        "referer": "https://tossinvest.com/",
        "user-agent": "Mozilla/5.0"
    }

# ✅ HDFS에서 최신 댓글 ID 보내오기 (파일 없으면 0 반환)
def load_latest_comment_id(client, path: str) -> int:
    if not client.status(path, strict=False):
        return 0  # 파일이 없으면 처음부터 수집
    with client.read(path, encoding="utf-8-sig") as f:
        return int(next(pd.read_csv(f, chunksize=1))["Comment ID"].iloc[0])

# ✅ 댓글 수집
def collect_recent_comments(stock_name: str, start_date_str: str, client, path: str) -> list:
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    latest_id = load_latest_comment_id(client, path)

    data = {
        "subjectId": STOCK_IDS[stock_name],
        "subjectType": "STOCK",
        "commentSortType": "RECENT"
    }
    headers = get_headers()
    comments = []

    print(f"🚀 {stock_name} 댓글 수집 시작 (from {start_date_str})")

    while True:
        res = requests.post("https://wts-cert-api.tossinvest.com/api/v3/comments", json=data, headers=headers)
        if res.status_code != 200:
            print(f"❌ 요청 실패: {res.status_code}")
            break

        chunk = res.json().get("result", {}).get("comments", {}).get("body", [])
        if not chunk:
            print("✅ 더 이상 수집할 댓글이 없습니다.")
            break

        stop = False
        for c in chunk:
            cid = int(c["id"])
            date = c["updatedAt"]

            if cid <= latest_id:
                print(f"🚓 최신 댓글 ID({cid}) 도달 → 수집 중지")
                stop = True
                break
            if datetime.strptime(date[:10], "%Y-%m-%d") < start_date:
                print(f"🚓 {date}는 시작일 이전 → 수집 중단")
                stop = True
                break

            comments.append([
                cid,
                c["message"],
                date,
                c["author"]["nickname"],
                "toss",
                stock_name
            ])

        if stop:
            break

        if len(comments) % 100 == 0:
            print(f"🔄 누적 수집: {len(comments)}")

        data["commentId"] = chunk[-1]["id"]
        time.sleep(1)

    return comments

# ✅ 댓글 저장
def save_to_hdfs_csv(client, path: str, stock_name: str, comments: list):
    df_new = pd.DataFrame(comments, columns=[
        "Comment ID", "Message", "Updated At", "Nickname", "Platform", "Stock Name"
    ])

    if client.status(path, strict=False):
        with client.read(path, encoding="utf-8-sig") as f:
            df_old = pd.read_csv(f)
        df_all = pd.concat([df_new, df_old], ignore_index=True)
    else:
        df_all = df_new

    # ✅ 중복 제거 (Comment ID 기준)
    df_all = df_all.drop_duplicates(subset=["Comment ID"])

    # ✅ 시간 역순 정렬
    df_all = df_all.sort_values(by="Updated At", ascending=False)

    with BytesIO() as buffer:
        df_all.to_csv(buffer, index=False, encoding='utf-8-sig', lineterminator='\n')
        buffer.seek(0)
        client.write(path, buffer, overwrite=True)

    print(f"✅ 댓글 {len(df_new)}개 저장 완료: {path}")


# ✅ 메인 실행
if __name__ == "__main__":
    client = get_client()
    stock_name = get_stock_name()
    start_date_str = cal_std_day(10, case=2)  # ← 하드코딩: 어제부터 수집
    comment_path = f"/project-root/data/_0_raw/{stock_name}_comments.csv"

    comments = collect_recent_comments(stock_name, start_date_str, client, comment_path)
    if comments:
        save_to_hdfs_csv(client, comment_path, stock_name, comments)
    else:
        print("⚠️ 수집된 댓글이 없습니다.")