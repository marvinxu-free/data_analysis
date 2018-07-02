
CREATE EXTERNAL TABLE `qiancheng_ios_pred`(
  `maxent_id` string COMMENT 'maxent_id')
COMMENT 'decision tree predict fraud maxent_id'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://hdnn/user/hive/warehouse/odata.db/qiancheng_ios_pred'
TBLPROPERTIES (
  'transient_lastDdlTime'='1505893459')


CREATE EXTERNAL TABLE `qiancheng_android_pred`(
  `maxent_id` string COMMENT 'maxent_id')
COMMENT 'decision tree predict fraud maxent_id'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://hdnn/user/hive/warehouse/odata.db/qiancheng_android_pred'
TBLPROPERTIES (
  'transient_lastDdlTime'='1505893459')

