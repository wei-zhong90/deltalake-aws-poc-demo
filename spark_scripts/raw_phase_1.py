import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.context import SparkConf
from awsglue.job import Job
# Import the packages
from delta import *
from pyspark.sql.session import SparkSession
from datetime import datetime

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, from_json, lit

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket_name', 'bootstrap_servers', 'topic1', 'topic2'])
data_bucket = args['bucket_name']
bootstrap_servers = args['bootstrap_servers']
topic1 = args['topic1']
topic2 = args['topic2']
checkpoint_bucket1 = f"s3://{data_bucket}/checkpoint/"
checkpoint_bucket2 = f"s3://{data_bucket}/membership_checkpoint/"
    
schema1 = StructType([ \
  StructField("order_id", IntegerType(), True), \
  StructField("order_owner", StringType(), True), \
  StructField("order_value", IntegerType(), True), \
  StructField("timestamp", TimestampType(), True), ])

schema2 = StructType([ \
  StructField("order_owner", StringType(), True), \
  StructField("membership", StringType(), True), \
  StructField("timestamp", TimestampType(), True), ])

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

class JobBase(object):
  fair_scheduler_config_file= "fairscheduler.xml"
  
  def get_options(self, bootstrap_servers, topic):
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
  
  def __start_spark_glue_context(self):
    conf = SparkConf().setAppName("python_thread").set('spark.scheduler.mode', 'FAIR').set("spark.scheduler.allocation.file", self.fair_scheduler_config_file)
    self.sc = SparkContext(conf=conf)
    self.glue_context = GlueContext(self.sc)
    self.spark = self.glue_context.spark_session
  
  def execute(self):
    self.__start_spark_glue_context()
    self.logger = self.glue_context.get_logger()
    self.logger.info("Starting Glue Threading job ")
    import concurrent.futures
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    executor.submit(self.workflow, topic1, schema1, insertToDelta1, checkpoint_bucket1)
    executor.submit(self.workflow, topic2, schema2, insertToDelta2, checkpoint_bucket2)
    self.logger.info("Completed Threading job")
    
    
  def workflow(self, topic, schema, insertToDelta, checkpoint_bucket):
    # Read Source
    df = self.spark \
      .readStream \
      .format("kafka") \
      .options(**self.get_options(bootstrap_servers, topic)) \
      .load().select(col("value").cast("STRING"))

    df2 = df.select(from_json("value", schema).alias("data")).select("data.*")


    # Write data as a DELTA TABLE
    df3 = df2.writeStream \
      .foreachBatch(insertToDelta) \
      .option("checkpointLocation", checkpoint_bucket) \
      .trigger(processingTime="60 seconds") \
      .start()

    df3.awaitTermination()

def main():
  job = JobBase()
  job.execute()


if __name__ == '__main__':
  main()
  

# df_t2 = spark \
#   .readStream \
#   .format("kafka") \
#   .options(**get_options(bootstrap_servers, topic2)) \
#   .load().select(col("value").cast("STRING"))

# df2_t2 = df_t2.select(from_json("value", schema2).alias("data")).select("data.*")


# # Write data as a DELTA TABLE
# df3_t2 = df2_t2.writeStream \
#   .foreachBatch(insertToDelta2) \
#   .option("checkpointLocation", f"s3://{data_bucket}/membership_checkpoint/") \
#   .trigger(processingTime="60 seconds") \
#   .start()

# df3_t2.awaitTermination()

# job.commit()

