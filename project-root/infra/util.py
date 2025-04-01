#######################################
# 프로젝트 실행에 사용할 util 함수
# 날짜생성, restapi 요청 등
from datetime import datetime, timedelta
import requests
import json
from infra.hdfs_client import get_client
import pandas as pd
import csv

def load_keyword_file(path: str, as_set: bool = False):
    client = get_client()
    with client.read(path, encoding="utf-8-sig") as f:
        lines = [line.strip() for line in f if line.strip()]
        return set(lines) if as_set else lines

#HDFS저장
def save_csv_to_hdfs(df: pd.DataFrame, path: str, client) -> None:
    with client.write(path, encoding='utf-8-sig', overwrite=True) as writer:
        df.to_csv(
            writer,
            index=False,
            encoding='utf-8-sig',
            quoting=csv.QUOTE_NONE
        )

# HDFS에 저장된 CSV파일 pandas로 읽어오기        
def load_csv_from_hdfs(client, path: str, encoding: str = "utf-8-sig") -> pd.DataFrame:
    with client.read(path, encoding=encoding) as reader:
        return pd.read_csv(reader)


def cal_std_day(before_day, case=1):
    x = datetime.now() - timedelta(before_day)
    year = x.year
    month = x.month if x.month >= 10 else '0'+ str(x.month)
    day = x.day if x.day >= 10 else '0' + str(x.day)
    if case == 1:
        return str(year)+str(month)+str(day) 
    else:
        return str(year) + '-' + str(month) + '-' + str(day) 

def executeRestApi(method, url, headers, params):  
    # params, headers는 dict로 구성되어야 함
    # raise Exception('응답코드 : 500')  # 예외 테스트
    # err_num = 10/0 # 예외 테스트
    if method == "get": # header가 필요없음 - 파라미터로 사용해야 된다면 보통 {} 빈디렉터리가 전달됨
        res = requests.get(url, params=params, headers=headers)
    else: # post 요청 : header 필요, header는 {}로 구성되어야 함
        res = requests.post(url, data=params, headers=headers)

    if res == None or res.status_code != 200:
        raise Exception('응답코드 : ' + str(res.status_code))
       
    return json.loads(res.text)


def create_conf() :
    conf_dm = {
        'url':'jdbc:mysql://djangomysql:3306/etlWeatherDM?characterEncoding=utf8&serverTimezone=Asia/Seoul'
        ,'props':{
            'user':'bigMysql',
            'password':'bigMysql1234@'   
        }
    }

    conf_svc = {
        'url':'jdbc:mysql://djangomysql:3306/etlWeatherSVC?characterEncoding=utf8&serverTimezone=Asia/Seoul'
        ,'props':{
        'user':'serviceAccount',
        'password':'qwe123!@#'   
        }
    }

    return [conf_dm,conf_svc]

if __name__ == '__main__':
    print(cal_std_day(1))