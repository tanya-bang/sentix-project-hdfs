0402
1. 크롤링, aggregate airflow확인
2. 크롤링 이후 삼성증권만 preprocess에서 오류나는데 이 부분 오류찾지않고 홀딩함.
3. 크롤링 코드 정돈 필요해보여서 일부 수정함
4. 다른 파일도 일부 수정있고 util도 수정있음

==========================================

0401 15시
1. raw데이터는 기존에 사용하던 데이터 이용

2. 코드 실행 명령어 (터미널에 실행)
for stock in apple samsung skhynix nvidia
do
PYTHONPATH=/root/project-root \
STOCK_NAME=$stock \
python /root/project-root/scripts/preprocess_main.py
done

for stock in apple samsung skhynix nvidia
do
PYTHONPATH=/root/project-root \
STOCK_NAME=$stock \
python /root/project-root/scripts/labeling_main.py
done

3. 전체적인 데이터 전처리 재정비 및 2글자 이상 데이터만 보존
✅ apple 전처리 완료! 남은 댓글 수: 41,118
✅ samsung 전처리 완료! 남은 댓글 수: 132,606
✅ skhynix 전처리 완료! 남은 댓글 수: 56,627
✅ nvidia 전처리 완료! 남은 댓글 수: 73,983

4. 라벨 분포
Apple (총 41,118개)
공포(0): 5,350개 → 13.0%
중립(1): 27,363개 → 66.6%
탐욕(2): 8,405개 → 20.4%

Samsung (총 132,606개)
공포(0): 19,135개 → 14.4%
중립(1): 88,302개 → 66.6%
탐욕(2): 25,169개 → 19.0%

SK Hynix (총 56,627개)
공포(0): 8,267개 → 14.6%
중립(1): 36,515개 → 64.5%
탐욕(2): 11,845개 → 20.9%

Nvidia (총 73,983개)
공포(0): 10,426개 → 14.1%
중립(1): 49,472개 → 66.9%
탐욕(2): 14,085개 → 19.0%