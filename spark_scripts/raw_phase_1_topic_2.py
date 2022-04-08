import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
# Import the packages
from delta import *
from pyspark.sql.session import SparkSession

from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, from_json

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket_name', 'bootstrap_servers', 'topic'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


data_bucket = args['bucket_name']
bootstrap_servers = args['bootstrap_servers']
topic = args['topic']
schema = StructType([ \
  StructField("order_owner", StringType(), True), \
  StructField("membership", StringType(), True), \
  StructField("timestamp", TimestampType(), True), ])

# Initialize Spark Session along with configs for Delta Lake
# spark = SparkSession \
#     .builder \
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#     .getOrCreate()

def insertToDelta(microBatch, batchId):
  if microBatch.count() > 0:
    microBatch.write.partitionBy("order_owner").mode("append").format("delta").save(f"s3://{data_bucket}/raw/membership/")
    

options = {
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

# Read Source
df = spark \
  .readStream \
  .format("kafka") \
  .options(**options) \
  .load().select(col("value").cast("STRING"))

df2 = df.select(from_json("value", schema).alias("data")).select("data.*")


# Write data as a DELTA TABLE
df3 = df2.writeStream \
  .foreachBatch(insertToDelta) \
  .option("checkpointLocation", f"s3://{data_bucket}/membership_checkpoint/") \
  .trigger(processingTime="60 seconds") \
  .start()

df3.awaitTermination()

job.commit()

