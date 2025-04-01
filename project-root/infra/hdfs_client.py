###################################################
# hdfs 접근과 관련된 함수
###################################################
from hdfs import InsecureClient
from infra import spark_session

# DL -> DW 저장 시 사용
def get_client() : 
    client = InsecureClient('http://master1:9870', user='root')
    #client = InsecureClient('http://master1:9870', user='root') 이렇게 master1으로 사용해도 됨
    return client

# DW -> DMDB 저장 시 사용
def get_HDFSdata(path, fname) :
    #file_name = 'hdfs://localhost:9000' + path + fname
    file_name = 'hdfs://master1:9000' + path + fname
    spark_rc = spark_session.get_spark_session()
    # print(spark_rc)
    weToDF = spark_rc.read.json(file_name, encoding='UTF-8') #spark.sql.DF 생성됨
    return weToDF