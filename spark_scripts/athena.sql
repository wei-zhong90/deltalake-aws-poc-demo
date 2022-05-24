CREATE EXTERNAL TABLE `processed`(
  `order_id` int, 
  `order_owner` string, 
  `order_value` int, 
  `timestamp` timestamp,
  `membership` string)
PARTITIONED BY ( 
  `year` string, 
  `month` string, 
  `day` string, 
  `hour` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://delta-lake-streaming-demo/processed/_symlink_format_manifest'
TBLPROPERTIES (
  'transient_lastDdlTime'='1648044703')