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


def calculate_ewma(values, com):
    alpha = 1 / (com + 1)
    ewma = values[0]
    for value in values[1:]:
        ewma = (1 - alpha) * ewma + alpha * value
    return ewma
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
ewma_udf = F.udf(calculate_ewma, T.DoubleType())


spark = SparkSession.builder.appName("binance")\
    .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/postgresql-42.7.1.jar") \
    .getOrCreate()



schema = T.StructType([
    T.StructField("datetime", T.TimestampType(), True),
    T.StructField("symbol", T.StringType(), True),
    T.StructField("open", T.DoubleType(), True),
    T.StructField("high", T.DoubleType(), True),
    T.StructField("low", T.DoubleType(), True),
    T.StructField("close", T.DoubleType(), True)
])



db = "coin"
sql = "SELECT * FROM kline_1d order by datetime asc"
kline_1d = spark.read.format("jdbc")\
                    .option("url","jdbc:postgresql://{0}:{1}/{2}".format( config.psql_ip, config.psql_port, db ) )\
                    .option("driver", "org.postgresql.Driver")\
                    .option("user", config.psql_user)\
                    .option("query", sql)\
                    .option("password", config.psql_passwd)\
                    .option("schema", schema)\
                    .load()
rsi_df = RSI(kline_1d)
rsi_df = rsi_df.orderBy("datetime", ascending =False)
res_df = rsi_df.select("datetime","symbol","open","high","low","close","up_ewma","down_ewma","rsi")

db = "coin"
dbtable = "kline_1d_rsi"
res_df.write.format("jdbc") \
    .option("url","jdbc:postgresql://{0}:{1}/{2}".format(config.psql_ip, config.psql_port, db ))\
    .option("driver", "org.postgresql.Driver") \
    .option("user", config.psql_user) \
    .option("password", config.psql_passwd) \
    .option("dbtable", dbtable) \
    .mode("overwrite") \
    .save()