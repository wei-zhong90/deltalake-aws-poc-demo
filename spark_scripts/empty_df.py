import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from delta import *
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# deltaTable = DeltaTable.forPath(spark, "s3://delta-lake-lego-demo/processed/")
# deltaTable.generate("symlink_format_manifest")

df = spark.read.format("delta").load("s3://delta-lake-lego-demo/processed/")

df2 = df.createOrReplaceTempView("generated_loads")

spark.sql("select * from generated_loads").show(df.count(), False)

# schema = StructType([ \
#   StructField("order_id", IntegerType(), True), \
#   StructField("order_owner", StringType(), True), \
#   StructField("order_value", IntegerType(), True), \
#   StructField("timestamp", StringType(), True), \
#   StructField("year", StringType(), True), \
#   StructField("month", StringType(), True), \
#   StructField("day", StringType(), True), \
#   StructField("hour", StringType(), True) \
#   ])

# rdd = spark.sparkContext.emptyRDD()

# df = spark.createDataFrame(rdd,schema)

# df.write.partitionBy("year", "month", "day", "hour").format("delta").mode("overwrite").save("s3://delta-lake-lego-demo/processed/")

# df = spark.sql("""CREATE TABLE default.processed (
# order_id INT,
# order_owner STRING,
# order_value INT,
# timestamp STRING,
# year STRING,
# month STRING,
# day STRING,
# hour STRING
# ) USING DELTA 
# PARTITIONED BY (year, month, day, hour) 
# LOCATION 's3://delta-lake-lego-demo/processed/'""")


job.commit()