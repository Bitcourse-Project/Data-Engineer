import time
import datetime
import pytz
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from decimal import Decimal
import pyspark.sql.types as T
from pyspark.sql import functions as F


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
    df = df.withColumn("diversions", F.lit(1))
    # producer send
    df.write.format("jdbc") \
    .option("url", "jdbc:postgresql://postgres-coin:5432/coin") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", user) \
    .option("password", passwd) \
    .option("dbtable", "now_diversions") \
    .mode("overwrite") \
    .save()

def set_state(n):
    
    data = [(n,)]
    columns = ["state"]
    task = spark.createDataFrame(data, columns)

    task.write.format("jdbc") \
    .option("url", "jdbc:postgresql://postgres-coin:5432/coin") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", user) \
    .option("password", passwd) \
    .option("dbtable", "task_state") \
    .mode("overwrite") \
    .save()

spark = SparkSession.builder.appName("binance")\
    .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/postgresql-42.7.1.jar") \
    .getOrCreate()


ip = "postgres-coin"
port = "5432"
db = "coin"
user = "postgres"
passwd = "postgres"
sql = "SELECT * FROM kline_1d_rsi ORDER BY datetime DESC LIMIT 30"

kline_1d_rsi = spark.read.format("jdbc")\
                    .option("url","jdbc:postgresql://{0}:{1}/{2}".format( ip, port, db ) )\
                    .option("driver", "org.postgresql.Driver")\
                    .option("user", user)\
                    .option("query", sql)\
                    .option("password", passwd)\
                    .load()



if up_diversions(kline_1d_rsi):
    # kafka send 추가
    set_diver_state(kline_1d_rsi, 1)
    set_state(1)
    

if down_diversions(kline_1d_rsi):
    # kafka send
    set_diver_state(kline_1d_rsi, 2)
    set_state(2)
