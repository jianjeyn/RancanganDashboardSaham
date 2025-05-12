import os
import subprocess
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, first, last, max, min, sum

load_dotenv('/opt/airflow/jobs/.env')

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB = os.getenv("MONGODB_DB")

subprocess.run(["python", "jobs/YFinance/_scrap.py",
                "--period", "1d",
                "--collection", "yfinance_d"
], check=True)

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
    .load().withColumn("date", to_date(col("Date")))

d_df = df.groupBy(
    "ticker",
    "date"
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