import time
import datetime
import pytz
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import to_timestamp, from_unixtime, from_utc_timestamp
from pyspark.sql.types import *
import os
from decimal import Decimal 

from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("binance")\
    .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/postgresql-42.7.1.jar") \
    .getOrCreate()

schema = StructType([
    StructField("datetime", TimestampType(), True),
    StructField("symbol", StringType(), True),
    StructField("open", DecimalType(18, 2), True),
    StructField("high", DecimalType(18, 2), True),
    StructField("low", DecimalType(18, 2), True),
    StructField("close", DecimalType(18, 2), True),
    StructField("volume", DecimalType(18, 2), True),
    StructField("QuoteAssetVolume", DecimalType(18, 2), True),
    StructField("NumTrades", IntegerType(), True),
    StructField("TakerBuyBaseAssetVolume", DecimalType(18, 2), True),
    StructField("TakerBuyQuoteAssetVolume", DecimalType(18, 2), True),
    StructField("Ignore", StringType(), True)
])


ip = "postgres-coin"
port = "5432"
db = "coin"
user = "postgres"
passwd = "postgres"
sql = "select * from kline_1day"



kline_1d = spark.read.format("jdbc")\
                    .option("url","jdbc:postgresql://{0}:{1}/{2}".format( ip, port, db ) )\
                    .option("driver", "org.postgresql.Driver")\
                    .option("user", user)\
                    .option("query", sql)\
                    .option("password", passwd)\
                    .schema(schema)\
                    .load()


k1 = kline_1d.orderBy("datetime", ascending=False).limit(14)


def RSI(df, period=14):

    windowSpec = Window.orderBy("datetime") 

    delta = F.col("close") - F.lag("close").over(windowSpec)
    
    delta.show()
    # ups = F.when(delta > 0, delta).otherwise(0).alias("ups")
    # downs = F.when(delta < 0, delta).otherwise(0).alias("downs")

    

    # # 지수 가중 이동 평균 계산
    # w = Window.orderBy("datetime").rowsBetween(-period, -1)
    # AU = F.avg(F.when(ups > 0, ups).otherwise(0)).over(w)
    # AD = F.avg(F.when(downs < 0, F.abs(downs)).otherwise(0)).over(w)


    # # RS 및 RSI 계산
    # RS = AU / AD
    # RSI = 100 - (100 / (1 + RS))

    # # DataFrame으로 반환
    # result_df = df.withColumn("RSI", RSI)
    # result_df.show()
    # return result_df.select("timestamp", "RSI")

def RSI2(df, period=14):
    # 이전 행의 종가를 가져옴
    df = df.orderBy("datetime", ascending=False).limit(14)

    df = df.orderBy("datetime", ascending=False)
    df = df.withColumn("prev_close", F.lag(df["close"]).over(Window.orderBy("datetime")))

    df = df.withColumn("delta", df["close"] - df["prev_close"])
    df.show()

    AU = df.select(F.avg(F.when(df["delta"] > 0, df["delta"]).otherwise(0)).alias("AU")).collect()[0]["AU"]
    AD = df.select(F.avg(F.when(df["delta"] < 0, F.abs(df["delta"])).otherwise(0)).alias("AD")).collect()[0]["AD"]

    print("AU:", AU)
    print("AD:", AD)


    RS = AU / AD
    print('RS',RS)
    RSI = 100 - (100 / (1 + RS))
    return RSI

print(RSI2(kline_1d))
