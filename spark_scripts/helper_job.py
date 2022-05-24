import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from delta import *
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#### Generate manifest file ####
# deltaTable = DeltaTable.forPath(spark, "s3://deltalake-poc-glue-wei/processed/")
# deltaTable.generate("symlink_format_manifest")


#### Check the processed result ####
# df = spark.read.format("delta").load("s3://deltalake-poc-glue-wei/processed/")
# df2 = df.createOrReplaceTempView("generated_loads")
# spark.sql("select * from generated_loads").show(df.count(), False)


#### Generate the empty result table ####
# spark.conf.set("delta.compatibility.symlinkFormatManifest.enabled", "true")
# schema = StructType([ \
#   StructField("order_id", IntegerType(), True), \
#   StructField("order_owner", StringType(), True), \
#   StructField("order_value", IntegerType(), True), \
#   StructField("timestamp", TimestampType(), True), \
#   StructField("membership", StringType(), True), \
#   StructField("year", StringType(), True), \
#   StructField("month", StringType(), True), \
#   StructField("day", StringType(), True), \
#   StructField("hour", StringType(), True) \
#   ])
# rdd = spark.sparkContext.emptyRDD()
# df = spark.createDataFrame(rdd,schema)
# df.write.partitionBy("year", "month", "day", "hour").format("delta").mode("overwrite").save("s3://deltalake-poc-glue-wei/processed/")


#### Compact the small files ####
path = "s3://deltalake-poc-glue-wei/raw/"
numFiles = 10

(spark.read
 .format("delta")
 .load(path)
 .repartition(numFiles)
 .write
 .option("dataChange", "false")
 .format("delta")
 .mode("overwrite")
 .save(path))


#### Compact a partition ####
path = "s3://deltalake-poc-glue-wei/raw/"
partition = "year = '22'"
numFilesPerPartition = 10

(spark.read
 .format("delta")
 .load(path)
 .where(partition)
 .repartition(numFilesPerPartition)
 .write
 .option("dataChange", "false")
 .format("delta")
 .mode("overwrite")
 .option("replaceWhere", partition)
 .save(path))

job.commit()