import os
import subprocess
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, first, last, max, min, sum

load_dotenv('/opt/airflow/jobs/.env')

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB = os.getenv("MONGODB_DB")

subprocess.run(["python", "jobs/YFinance/_scrap.py"], check=True)

_spark = SparkSession.builder \
    .appName("ReadMongo") \
    .config("spark.jars", "/opt/airflow/jars/mongo-spark-connector.jar,"
                          "/opt/airflow/jars/mongodb-driver-sync.jar,"
                          "/opt/airflow/jars/mongodb-driver-core.jar,"
                          "/opt/airflow/jars/bson.jar") \
    .config("spark.mongodb.read.connection.uri", MONGODB_URI) \
    .getOrCreate()

df = _spark.read.format("mongo") \
    .option("uri", MONGODB_URI) \
    .option("database", MONGODB_DB) \
    .option("collection", "yfinance") \
    .load().withColumn("Date", to_date(col("Date")))

last_day = df.select("Date").distinct().orderBy(col("Date").desc()).first()[0]
last_day_df = df.filter(col("Date") == last_day)

d_df = last_day_df.groupBy(
    "ticker", "Date"
).agg(
    first("Open").alias("Open"),
    max("High").alias("High"),
    min("Low").alias("Low"),
    last("Close").alias("Close"),
    sum("Volume").alias("Volume")
)

d_df.write.format("mongo")\
    .option("uri", MONGODB_URI) \
    .option("database", MONGODB_DB) \
    .option("collection", "yfinance_d") \
    .option("replaceDocument", "false") \
    .mode("append") \
    .save()
