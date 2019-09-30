add jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=true;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;



CREATE DATABASE IF NOT EXISTS hob201934_kkt;

DROP TABLE IF EXISTS hob201934_kkt.raw;
CREATE external TABLE hob201934_kkt.raw (
  fsId string,
  content struct<totalSum: int, userInn: string>,
  ofdId string,
  subtype string,
  documentId int,
  protocolSubversion int,
  receiveDate string,
  `_id` string,
  kktRegId string,
  protocolVersion int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ("ignore.malformed.json" = "true")
LOCATION '/data/hive/fns2';

SELECT content.userInn as user, content.totalSum as sum FROM hob201934_kkt.raw WHERE subtype = "receipt" LIMIT 10;

DROP TABLE IF EXISTS hob201934_kkt.u_parquet;
CREATE TABLE hob201934_kkt.u_parquet STORED AS PARQUET AS
    SELECT content.userInn as user, content.totalSum as sum FROM hob201934_kkt.raw WHERE subtype = "receipt";


SELECT user, SUM(t.sum) as ss FROM hob201934_kkt.u_parquet as t GROUP BY user ORDER BY ss DESC LIMIT 1;

