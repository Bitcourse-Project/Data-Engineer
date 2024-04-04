import sys
import time
import datetime
import pytz
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from decimal import Decimal
import pyspark.sql.types as T
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import config
from kafka import KafkaProducer

# state 확인
def get_state():
    db = "coin"
    sql = "select * from task_state"
    df = spark.read.format("jdbc")\
                    .option("url","jdbc:postgresql://{0}:{1}/{2}".format(config.psql_ip, config.psql_port, db ) )\
                    .option("driver", "org.postgresql.Driver")\
                    .option("user", config.psql_user)\
                    .option("query", sql)\
                    .option("password", config.psql_passwd)\
                    .load()
    state = df.first()["state"]
    return state

def set_state(n):
    data = [(n,)]
    columns = ["state"]
    task = spark.createDataFrame(data, columns)
    db = "coin"
    task.write.format("jdbc") \
        .option("url","jdbc:postgresql://{0}:{1}/{2}".format(config.psql_ip, config.psql_port, db ) )\
        .option("driver", "org.postgresql.Driver") \
        .option("user", config.psql_user)\
        .option("password", config.psql_passwd)\
        .option("dbtable", "task_state") \
        .mode("overwrite") \
        .save()

def pc(a,b):
    if a == b:
        return 0
    else:
        return ((b-a)/a) * 100


spark = SparkSession.builder.appName("binance")\
    .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/postgresql-42.7.1.jar") \
    .getOrCreate()


if get_state() == 0:
    print("다이버전스가 확인되지 않았습니다.")
    sys.exit()

# topic = 'coin'
# kafka_bootstrap_servers = 'kafka-div:9092'
# producer = KafkaProducer(bootstrap_servers=[kafka_bootstrap_servers])

# 가장 최근 수집 데이터 불러오기
sql = "SELECT * FROM kline_1d order by datetime DESC limit 1"
db = "coin"
now_kline_1d = spark.read.format("jdbc")\
                    .option("url","jdbc:postgresql://{0}:{1}/{2}".format( config.psql_ip, config.psql_port, db ) )\
                    .option("driver", "org.postgresql.Driver")\
                    .option("user", config.psql_user)\
                    .option("query", sql)\
                    .option("password", config.psql_passwd)\
                    .load()

# 현재 다이버전스 데이터 불러오기
sql = "SELECT * FROM now_diversions"
now_div = spark.read.format("jdbc")\
                    .option("url","jdbc:postgresql://{0}:{1}/{2}".format( config.psql_ip, config.psql_port, db ) )\
                    .option("driver", "org.postgresql.Driver")\
                    .option("user", config.psql_user)\
                    .option("query", sql)\
                    .option("password", config.psql_passwd)\
                    .load()

n = now_div.first()["diversions"]
d1 = now_kline_1d.first()["datetime"]
d2 = now_div.first()["datetime"]
time_difference = d1 - d2
print(time_difference, type(time_difference))
print(time_difference.days, type(time_difference.days))

c1 = now_kline_1d.first()["close"]
c2 = now_div.first()["close"]
convert = pc(c1, c2)
print(convert)

if convert >= 4.3 and n == 1:
    now_div = now_div.withColumn("convert", F.lit(1))
    now_div.show()
    #producer.send()
    #set_state(0)

if convert <= -4.3 and n == 2:
    now_div = now_div.withColumn("convert", F.lit(1))
    now_div.show()
    #producer.send()
    #set_state(0)


# 이틀 지나면 초기화
if time_difference.days >= 2:
    set_state(0)
    pass