CREATE EXTERNAL TABLE IF NOT EXISTS processed (
`order_id` INT,
`order_owner` STRING,
`order_value` INT,
`timestamp` INT
) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 

STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://delta-lake-lego-demo/processed/_symlink_format_manifest/'