create external schema lego_schema 
from data catalog 
database 'myspectrum_db' 
iam_role 'arn:aws-cn:iam::592757762710:role/test-redshift-role'
create external database if not exists;

CREATE EXTERNAL TABLE lego_schema.testtable (order_id INT,order_owner VARCHAR(20),order_value INT,"timestamp" TIMESTAMP)
PARTITIONED BY ("year" VARCHAR(20), "month" VARCHAR(20), "day" VARCHAR(20), "hour" VARCHAR(20))
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS 
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://delta-lake-lego-demo/processed/_symlink_format_manifest';

ALTER TABLE lego_schema.testtable ADD IF NOT EXISTS PARTITION ("year"='22', "month"='03', "day"='15', "hour"='05') LOCATION 's3://delta-lake-lego-demo/processed/_symlink_format_manifest/year=22/month=03/day=15/hour=05/';

select order_id, order_value from lego_schema.testtable;