import time
import datetime
import pytz
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from decimal import Decimal
import pyspark.sql.types as T
from pyspark.sql import functions as F
from kafka import KafkaProducer
import config
import sys
from kafka import KafkaProducer


def up_diversions(df):
    last_row = df.limit(1)
    last_row.show()
    last_row_rsi = last_row.select("rsi").collect()[0]["rsi"]
    last_row_low = last_row.select("low").collect()[0]["low"]

    # 현재 행 중복제거
    df = df.filter(F.col("rsi") <= 30)
    df = df.filter((F.col("rsi") <= last_row_rsi) & (F.col("low") > last_row_low))

    #df.select("datetime","low", "close","up_ewma","down_ewma","rsi").orderBy("datetime").show()
    if df.count() > 0:
        return True
    
    return False

def down_diversions(df):
    last_row = df.limit(1)
    last_row_rsi = last_row.select("rsi").collect()[0]["rsi"]
    last_row_high = last_row.select("high").collect()[0]["high"]
    
    df = df.filter(F.col("rsi") >= 70)
    df = df.filter((F.col("rsi") >= last_row_rsi) & (F.col("high") < last_row_high))
    df.select("datetime","high", "close","up_ewma","down_ewma","rsi").orderBy("datetime").show()
    
    if df.count() > 0:
        return True
    
    return False

def set_diver_state(df, n):
    df = df.select("datetime","symbol","close").limit(1)
    df = df.withColumn("diversions", F.lit(n))
    df = df.withColumn("convert", F.lit(0))
    db = "coin"
    # producer send
    df.write.format("jdbc") \
    .option("url","jdbc:postgresql://{0}:{1}/{2}".format(config.psql_ip, config.psql_port, db ) )\
    .option("driver", "org.postgresql.Driver") \
    .option("user", config.psql_user) \
    .option("password", config.psql_passwd) \
    .option("dbtable", "now_diversions") \
    .mode("overwrite") \
    .save()

    return df

def get_state():
    db = "coin"
    sql = "select * from task_state"
    df = spark.read.format("jdbc")\
                    .option("url","jdbc:postgresql://{0}:{1}/{2}".format(config.psql_ip, config.psql_port, db ) )\
                    .option("driver", "org.postgresql.Driver")\
                    .option("user", config.psql_user) \
                    .option("password", config.psql_passwd) \
                    .option("query", sql)\
                    .load()
    df.show()
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
    .option("user", config.psql_user) \
    .option("password", config.psql_passwd) \
    .option("dbtable", "task_state") \
    .mode("overwrite") \
    .save()

def send_message(df):
    # 수정필요
    json_strings = df.toJSON().collect()
    #producer.send(topic, row.encode('utf-8'))

spark = SparkSession.builder.appName("binance")\
    .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/postgresql-42.7.1.jar") \
    .getOrCreate()

if get_state():
    print("다이버전스가 이미 떠 있습니다.")
    sys.exit()

# topic = 'coin'
# kafka_bootstrap_servers = 'kafka-div:9092'
# producer = KafkaProducer(bootstrap_servers=[kafka_bootstrap_servers])


ip = "postgres-coin"
port = "5432"
db = "coin"
user = "postgres"
passwd = "postgres"
sql = "SELECT * FROM kline_1d_rsi ORDER BY datetime DESC LIMIT 30"

kline_1d_rsi = spark.read.format("jdbc")\
                    .option("url","jdbc:postgresql://{0}:{1}/{2}".format(config.psql_ip, config.psql_port, db ) )\
                    .option("driver", "org.postgresql.Driver")\
                    .option("user", config.psql_user) \
                    .option("password", config.psql_passwd) \
                    .option("query", sql)\
                    .load()


if up_diversions(kline_1d_rsi):
    # kafka send 추가
    df = set_diver_state(kline_1d_rsi, 1)
    set_state(1)
    #send_message(df)
    
if down_diversions(kline_1d_rsi):
    # kafka send
    df = set_diver_state(kline_1d_rsi, 2)
    set_state(1)
    #send_message(df)

