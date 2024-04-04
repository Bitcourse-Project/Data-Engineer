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
import config
from decimal import Decimal  # decimal 모듈 임포트 추가


spark = SparkSession.builder.appName("binance")\
    .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/postgresql-42.7.1.jar") \
    .getOrCreate()

def getData(api_key, api_secret, symbol):

    client = Client(api_key, api_secret)
    candles = client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_1DAY, limit = 2)
    candles = candles[0]

    korea_tz = pytz.timezone('Asia/Seoul')
    dt_object = datetime.datetime.fromtimestamp(candles[0]/1000, tz=pytz.utc).astimezone(korea_tz)

    data = {
        "datetime": dt_object,
        "symbol" : 'BTCUSDT',
        "open": float(candles[1]),
        "high": float(candles[2]),
        "low": float(candles[3]),
        "close": float(candles[4]),
        "volume": float(candles[5]),
        "quoteAssetVolume": float(candles[7]),
        "NumTrades": int(candles[8]),
        "TakerBuyBaseAssetVolume": float(candles[9]),
        "TakerBuyQuoteAssetVolume": float(candles[10])
    }
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
    ])


    row = Row(**data)
    df = spark.createDataFrame([row],schema)
    df = df.withColumn("datetime", from_utc_timestamp(df["datetime"], "Asia/Seoul"))
    return df

symbol = 'BTCUSDT'
df = getData(config.api_key, config.api_secret, symbol)

db = "coin"

df.write.format("jdbc") \
    .option("url","jdbc:postgresql://{0}:{1}/{2}".format(config.psql_ip, config.psql_port, db ) )\
    .option("driver", "org.postgresql.Driver") \
    .option("user", config.psql_user) \
    .option("password", config.psql_passwd) \
    .option("dbtable", "kline_1d") \
    .mode("append") \
    .save()