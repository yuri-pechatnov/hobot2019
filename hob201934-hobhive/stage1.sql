add jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=true;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;



CREATE DATABASE IF NOT EXISTS hob201934_kkt;

DROP TABLE IF EXISTS hob201934_kkt.raw;
CREATE external TABLE hob201934_kkt.raw (
  fsId string,
  content string,
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
LOCATION '/data/hive/fns2';

SELECT * FROM hob201934_kkt.raw LIMIT 100;
