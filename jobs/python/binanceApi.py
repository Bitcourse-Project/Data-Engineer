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

t = os.getenv("binance_api_key")
print(t)

api_key = config.api_key
api_secret = config.api_secret

client = Client(api_key, api_secret)

symbol = 'BTCUSDT'
candles = client.get_klines(symbol=symbol, interval=Client.KLINE_INTERVAL_1HOUR, limit = 1)
candles = candles[0]


korea_tz = pytz.timezone('Asia/Seoul')
dt_object = datetime.datetime.fromtimestamp(candles[0]/1000, tz=pytz.utc).astimezone(korea_tz)
print(dt_object)

data = {
    "datetime": dt_object,
    "symbol" : 'BTCUSDT',
    "open": Decimal(candles[1]),
    "high": Decimal(candles[2]),
    "low": Decimal(candles[3]),
    "close": Decimal(candles[4]),
    "volume": Decimal(candles[5]),
    "quoteAssetVolume": Decimal(candles[7]),
    "NumTrades": int(candles[8]),
    "TakerBuyBaseAssetVolume": Decimal(candles[9]),
    "TakerBuyQuoteAssetVolume": Decimal(candles[10])
}


schema = StructType([
    StructField("datetime", TimestampType(), True),
    StructField("symbol", StringType(), True),
    StructField("open", DecimalType(18, 2), True),
    StructField("high", DecimalType(18, 2), True),
    StructField("low", DecimalType(18, 2), True),
    StructField("close", DecimalType(18, 2), True),
    StructField("volume", DecimalType(18, 2), True),
    StructField("quoteassetvolume", DecimalType(18, 2), True),
    StructField("numtrades", IntegerType(), True),
    StructField("takerbuybaseassetvolume", DecimalType(18, 2), True),
    StructField("takerbuyquoteassetvolume", DecimalType(18, 2), True)
])

row = Row(**data)


df = spark.createDataFrame([row],schema)
df = df.withColumn("datetime", from_utc_timestamp(df["datetime"], "Asia/Seoul"))

# localhost
# port
port = "5432" 
# username
user = "postgres"
# password
passwd = "postgres"
# database
db = "coin"
url = "jdbc:postgresql://postgres-coin:5432/coin"

df.printSchema()
df.show()

df.write.format("jdbc") \
    .option("url", "jdbc:postgresql://postgres-coin:5432/coin") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", user) \
    .option("password", passwd) \
    .option("dbtable", "kline") \
    .mode("append") \
    .save()

# jdbc_properties = {
#     "user": user,
#     "password": passwd,
#     "driver": "org.postgresql.Driver"
# }

# # PostgreSQL 데이터 읽기
# df = spark.read.jdbc(url=url, table="kline", properties=jdbc_properties)
# df.show()