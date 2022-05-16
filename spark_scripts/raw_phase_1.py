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
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket_name', 'bootstrap_servers', 'topic1', 'topic2'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


data_bucket = args['bucket_name']
bootstrap_servers = args['bootstrap_servers']
topic1 = args['topic1']
topic2 = args['topic2']
schema1 = StructType([ \
  StructField("order_id", IntegerType(), True), \
  StructField("order_owner", StringType(), True), \
  StructField("order_value", IntegerType(), True), \
  StructField("timestamp", TimestampType(), True), ])

schema2 = StructType([ \
  StructField("order_owner", StringType(), True), \
  StructField("membership", StringType(), True), \
  StructField("timestamp", TimestampType(), True), ])

# Initialize Spark Session along with configs for Delta Lake
# spark = SparkSession \
#     .builder \
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#     .getOrCreate()

def insertToDelta1(microBatch, batchId):  
  date = datetime.today()
  year = date.strftime("%y")
  month = date.strftime("%m")
  day = date.strftime("%d")
  hour = date.strftime("%H")
  if microBatch.count() > 0:
    df = microBatch.withColumn("year", lit(year)).withColumn("month", lit(month)).withColumn("day", lit(day)).withColumn("hour", lit(hour))
    df.write.partitionBy("year", "month", "day", "hour").mode("append").format("delta").save(f"s3://{data_bucket}/raw/")

def insertToDelta2(microBatch, batchId):
      if microBatch.count() > 0:
        microBatch.write.partitionBy("order_owner").mode("append").format("delta").save(f"s3://{data_bucket}/raw/membership/")
    
 
def get_options(bootstrap_servers, topic):
      return {
        "kafka.bootstrap.servers": bootstrap_servers,
        "subscribe": topic,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "AWS_MSK_IAM", 
        "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
        "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
        "startingOffsets": "earliest",
        "maxOffsetsPerTrigger": 1000,
        "failOnDataLoss": "false"
      }
# options = {
#     "kafka.bootstrap.servers": bootstrap_servers,
#     "subscribe": topic,
#     "kafka.security.protocol": "SASL_SSL",
#     "kafka.sasl.mechanism": "AWS_MSK_IAM", 
#     "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
#     "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
#     "startingOffsets": "earliest",
#     "maxOffsetsPerTrigger": 1000,
#     "failOnDataLoss": "false"
#     }

# Read Source
df = spark \
  .readStream \
  .format("kafka") \
  .options(**get_options(bootstrap_servers, topic1)) \
  .load().select(col("value").cast("STRING"))

df2 = df.select(from_json("value", schema1).alias("data")).select("data.*")


# Write data as a DELTA TABLE
df3 = df2.writeStream \
  .foreachBatch(insertToDelta1) \
  .option("checkpointLocation", f"s3://{data_bucket}/checkpoint/") \
  .trigger(processingTime="60 seconds") \
  .start()

df3.awaitTermination()

df_t2 = spark \
  .readStream \
  .format("kafka") \
  .options(**get_options(bootstrap_servers, topic2)) \
  .load().select(col("value").cast("STRING"))

df2_t2 = df_t2.select(from_json("value", schema2).alias("data")).select("data.*")


# Write data as a DELTA TABLE
df3_t2 = df2_t2.writeStream \
  .foreachBatch(insertToDelta2) \
  .option("checkpointLocation", f"s3://{data_bucket}/membership_checkpoint/") \
  .trigger(processingTime="60 seconds") \
  .start()

df3_t2.awaitTermination()

job.commit()

