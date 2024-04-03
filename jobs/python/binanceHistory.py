import time
from binance import Client
import datetime
import pytz
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import *
from pyspark.sql.functions import from_unixtime, from_utc_timestamp
import os
from decimal import Decimal  # decimal 모듈 임포트 추가
import config


def convert_klines_to_rows(klines, symbol):
    rows = []
    for data in klines:
        korea_tz = pytz.timezone('Asia/Seoul')
        dt_object = datetime.datetime.fromtimestamp(data[0]/1000, tz=korea_tz)
        row = (
            dt_object,
            symbol,
            float(data[1]),  # 변환
            float(data[2]),  # 변환
            float(data[3]),  # 변환
            float(data[4]),  # 변환
            float(data[5]),  # 변환
            float(data[7]),  # 변환
            data[8],
            float(data[9]),  # 변환
            float(data[10]), # 변환
            data[11]
        )
        rows.append(row)
    return rows


def create_spark_dataframe(rows, schema):
    df_spark = spark.createDataFrame(rows, schema=schema)
    df_spark = df_spark.withColumn("datetime", from_utc_timestamp(df_spark["datetime"], "Asia/Seoul"))

    return df_spark

spark = SparkSession.builder.appName("binance")\
    .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/postgresql-42.7.1.jar") \
    .getOrCreate()

api_key = config.api_key
api_secret = config.api_secret
client = Client(api_key, api_secret)
symbol = 'BTCUSDT'

#klines = client.get_historical_klines(symbol, Client.KLINE_INTERVAL_1HOUR,"1 Jan, 2017")
#klines = client.get_historical_klines(symbol, Client.KLINE_INTERVAL_4HOUR,"1 Jan, 2017")
klines = client.get_historical_klines(symbol, Client.KLINE_INTERVAL_1DAY,"1 Jan, 2017")

# schema = StructType([
#     StructField("datetime", TimestampType(), True),
#     StructField("symbol", StringType(), True),
#     StructField("open", DecimalType(18, 2), True),
#     StructField("high", DecimalType(18, 2), True),
#     StructField("low", DecimalType(18, 2), True),
#     StructField("close", DecimalType(18, 2), True),
#     StructField("volume", DecimalType(18, 2), True),
#     StructField("QuoteAssetVolume", DecimalType(18, 2), True),
#     StructField("NumTrades", IntegerType(), True),
#     StructField("TakerBuyBaseAssetVolume", DecimalType(18, 2), True),
#     StructField("TakerBuyQuoteAssetVolume", DecimalType(18, 2), True),
#     StructField("Ignore", StringType(), True)
# ])
schema = StructType([
    StructField("datetime", TimestampType(), True),
    StructField("symbol", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("QuoteAssetVolume", DoubleType(), True),
    StructField("NumTrades", IntegerType(), True),
    StructField("TakerBuyBaseAssetVolume", DoubleType(), True),
    StructField("TakerBuyQuoteAssetVolume",DoubleType(), True),
    StructField("Ignore", StringType(), True)
])


rows = convert_klines_to_rows(klines,symbol)
df_spark = create_spark_dataframe(rows, schema)


port = "5432" 
user = "postgres"
passwd = "postgres"
db = "coin"
url = "jdbc:postgresql://postgres-coin:5432/coin"

df_spark.write.format("jdbc") \
    .option("url", "jdbc:postgresql://postgres-coin:5432/coin") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", user) \
    .option("password", passwd) \
    .option("dbtable", "kline_1d") \
    .mode("overwrite") \
    .save()

jdbc_properties = {
    "user": user,
    "password": passwd,
    "driver": "org.postgresql.Driver"
}
