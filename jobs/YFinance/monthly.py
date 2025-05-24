import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, first, last, max, min, sum

load_dotenv('/opt/airflow/jobs/.env')

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB = os.getenv("MONGODB_DB")

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
    .load().withColumn("Date", to_date(col("Date"))) \
    .withColumn("Year", year("Date")).withColumn("Month", month("Date"))

last_month = df.select("Year", "Month").distinct().orderBy(col("Year").desc(), col("Month").desc()).first()
last_month_df = df.filter((col("Year") == last_month[0]) & (col("Month") == last_month[1]))

m_df = last_month_df.groupBy(
    "ticker", "Year", "Month"
).agg(
    first("Open").alias("Open"),
    max("High").alias("High"),
    min("Low").alias("Low"),
    last("Close").alias("Close"),
    sum("Volume").alias("Volume")
)

m_df.write.format("mongo")\
    .option("uri", MONGODB_URI) \
    .option("database", MONGODB_DB) \
    .option("collection", "yfinance_m") \
    .option("replaceDocument", "false") \
    .mode("append") \
    .save()
