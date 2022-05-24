import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import scala.collection.JavaConverters._
import io.delta.tables.DeltaTable
import org.apache.spark.sql.expressions.Window

import scala.collection.JavaConverters._


object GlueApp {
  def main(sysArgs: Array[String]): Unit = {
    
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    val sparkSession: SparkSession = glueContext.getSparkSession
    import sparkSession.implicits._
    // @params: [JOB_NAME]
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "bucket_name").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    val CheckpointDir = s"s3://${args("bucket_name")}/checkpoint2"

    val raworder = sparkSession.readStream.format("delta").load(s"s3://${args("bucket_name")}/raw/order/").withWatermark("timestamp", "2 hours")
    val rawmember= sparkSession.readStream.format("delta").load(s"s3://${args("bucket_name")}/raw/member/").withWatermark("timestamp", "3 hours") 
  

   val joinedorder = raworder.alias("order").join(
         rawmember.alias("member"),
         expr("""
           order.order_owner = member.order_owner AND
           order.timestamp >= member.timestamp AND
           order.timestamp <= member.timestamp + interval 1 hour
         """)
   ).select($"order.order_id", $"order.order_owner", $"order.order_value", $"order.timestamp", $"member.membership", $"order.year", $"order.month", $"order.day", $"order.hour")


    val query = joinedorder
      .writeStream
      .format("delta")
      .option("checkpointLocation", CheckpointDir)
      .trigger(Trigger.ProcessingTime("60 seconds"))
      .outputMode("append")
      .partitionBy("year", "month", "day", "hour")
      .start(s"s3://${args("bucket_name")}/curated/")

    query.awaitTermination()   
 
    
    Job.commit()

  } 

}