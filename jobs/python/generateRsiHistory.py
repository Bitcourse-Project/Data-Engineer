import time
import datetime
import pytz
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from decimal import Decimal
import pyspark.sql.types as T
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def calculate_ewma(values, com):
    alpha = 1 / (com + 1)
    ewma = values[0]
    for value in values[1:]:
        ewma = (1 - alpha) * ewma + alpha * value
    return ewma
ewma_udf = F.udf(calculate_ewma, T.DoubleType())

def RSI(df, period=14, com=13):

    df.orderBy("datetime", ascending=True)
    windowSpec = Window.orderBy("datetime")
    df = df.withColumn("prev_close", F.lag("close", 1).over(windowSpec))

    df = df.withColumn("delta", F.col("close") - F.col("prev_close"))
    df = df.withColumn("up", F.when(F.col("delta") > 0, F.col("delta")).otherwise(0))
    df = df.withColumn("down", F.when(F.col("delta") < 0, -F.col("delta")).otherwise(0))

    df = df.withColumn("up_ewma", ewma_udf(F.collect_list("up").over(windowSpec), F.lit(com)))
    df = df.withColumn("down_ewma", ewma_udf(F.collect_list("down").over(windowSpec), F.lit(com)))
    
    # 상대적 강도 지수(RSI) 계산
    df = df.withColumn("rs", df.up_ewma / df.down_ewma)
    df = df.withColumn("rsi", 100 - (100 / (1 + df.rs)))

    return df


spark = SparkSession.builder.appName("binance")\
    .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/postgresql-42.7.1.jar") \
    .getOrCreate()


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

# ewma_udf = F.udf(calculate_ewma, T.DecimalType(18,2))

ip = "postgres-coin"
port = "5432"
db = "coin"
user = "postgres"
passwd = "postgres"
sql = "SELECT * FROM kline_1d"

kline_1d = spark.read.format("jdbc")\
                    .option("url","jdbc:postgresql://{0}:{1}/{2}".format( ip, port, db ) )\
                    .option("driver", "org.postgresql.Driver")\
                    .option("user", user)\
                    .option("query", sql)\
                    .option("password", passwd)\
                    .load()


rsi_df = RSI(kline_1d)
rsi_df = rsi_df.orderBy("datetime", ascending =False).limit(30)
rsi_df.printSchema()
rsi_df.select("datetime", "close","up_ewma","down_ewma","rsi").orderBy("datetime").show()