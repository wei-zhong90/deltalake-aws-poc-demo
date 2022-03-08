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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
from pyspark.sql.functions import from_json, unix_timestamp
from pyspark.sql.functions import col, from_json

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
  StructField("timestamp", StringType(), True), ])

# Initialize Spark Session along with configs for Delta Lake
# spark = SparkSession \
#     .builder \
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#     .getOrCreate()

date = datetime.today()
year = date.strftime("%y")
month = date.strftime("%m")
day = date.strftime("%d")
hour = date.strftime("%h")

# Read Source
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "b-1.test-cluster.dtvzzv.c4.kafka.cn-north-1.amazonaws.com.cn:9092,b-2.test-cluster.dtvzzv.c4.kafka.cn-north-1.amazonaws.com.cn:9092") \
  .option("subscribe", "test") \
  .load().select(col("value").cast("STRING"))

df2 = df.select(from_json("value", schema).alias("json"))

# Write data as a DELTA TABLE
df3 = df2.writeStream \
  .format("delta") \
  .option("checkpointLocation", "s3://delta-lake-lego-demo/checkpoint/") \
  .trigger(processingTime="60 seconds") \
  .outputMode("append") \
  .start(f"s3://delta-lake-lego-demo/raw/year={year}/month={month}/day={day}/hour={hour}/")

df3.awaitTermination()

job.commit()

