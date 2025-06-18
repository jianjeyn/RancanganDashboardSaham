import os
from pendulum import now
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, count

# Load environment variables
load_dotenv('/opt/airflow/jobs/.env')

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB = os.getenv("MONGODB_DB")

_spark = SparkSession.builder \
    .appName("IQPlusMonthlyAgg") \
    .config("spark.jars", "/opt/airflow/jars/mongo-spark-connector.jar,"
                          "/opt/airflow/jars/mongodb-driver-sync.jar,"
                          "/opt/airflow/jars/mongodb-driver-core.jar,"
                          "/opt/airflow/jars/bson.jar") \
    .config("spark.mongodb.read.connection.uri", MONGODB_URI) \
    .getOrCreate()

df = _spark.read.format("mongo") \
    .option("uri", MONGODB_URI) \
    .option("database", MONGODB_DB) \
    .option("collection", "BERITA_IQPLUS") \
    .load().withColumn("tanggal", to_date(col("tanggal"))) \
    .withColumn("Year", year("tanggal")).withColumn("Month", month("tanggal"))

last_time = now("Asia/Jakarta").subtract(months=1)
last_month_df = df.filter((col("Year") == last_time.year) & (col("Month") == last_time.month))

agg_df = last_month_df.groupBy("Year", "Month").agg(count("judul").alias("jumlah_berita"))

agg_df.write.format("mongo")\
    .option("uri", MONGODB_URI) \
    .option("database", MONGODB_DB) \
    .option("collection", "BERITA_IQPLUS_M") \
    .option("replaceDocument", "false") \
    .mode("append") \
    .save()
