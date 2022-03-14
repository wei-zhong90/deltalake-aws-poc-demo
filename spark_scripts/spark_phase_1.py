import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
# Import the packages
from delta import *
from pyspark.sql.session import SparkSession
from datetime import datetime

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, from_json, lit

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

schema = StructType([ \
  StructField("order_id", IntegerType(), True), \
  StructField("order_owner", StringType(), True), \
  StructField("order_value", IntegerType(), True), \
  StructField("timestamp", TimestampType(), True), ])

# Initialize Spark Session along with configs for Delta Lake
# spark = SparkSession \
#     .builder \
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#     .getOrCreate()

def insertToDelta(microBatch, batchId):  
  date = datetime.today()
  year = date.strftime("%y")
  month = date.strftime("%m")
  day = date.strftime("%d")
  hour = date.strftime("%H")
  if microBatch.count() > 0:
    df = microBatch.withColumn("year", lit(year)).withColumn("month", lit(month)).withColumn("day", lit(day)).withColumn("hour", lit(hour))
    df.write.partitionBy("year", "month", "day", "hour").mode("append").format("delta").save("s3://delta-lake-lego-demo/raw/")
    

# Read Source
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "b-1.test-cluster.dtvzzv.c4.kafka.cn-north-1.amazonaws.com.cn:9092,b-2.test-cluster.dtvzzv.c4.kafka.cn-north-1.amazonaws.com.cn:9092") \
  .option("subscribe", "lego") \
  .option("startingOffsets", "earliest") \
  .option("maxOffsetsPerTrigger", 1000) \
  .load().select(col("value").cast("STRING"))

df2 = df.select(from_json("value", schema).alias("data")).select("data.*")


# Write data as a DELTA TABLE
df3 = df2.writeStream \
  .foreachBatch(insertToDelta) \
  .option("checkpointLocation", "s3://delta-lake-lego-demo/checkpoint/") \
  .trigger(processingTime="60 seconds") \
  .start()

df3.awaitTermination()

job.commit()

