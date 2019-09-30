add jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;
ADD FILE ./best_day.py;
ADD FILE ./cat.py;
ADD FILE ./first_half_of_day.py;

SET hive.cli.print.header=true;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;



CREATE DATABASE IF NOT EXISTS hob201934_kkt;

DROP TABLE IF EXISTS hob201934_kkt.raw;
CREATE external TABLE hob201934_kkt.raw (
  fsId string,
  content struct<totalSum: int, userInn: string, dateTime: struct<ddate: string>, kktRegId: string>,
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
WITH SERDEPROPERTIES ("ignore.malformed.json" = "true", "mapping.ddate" = "$date")
LOCATION '/data/hive/fns2';

DROP TABLE IF EXISTS hob201934_kkt.u_text;
CREATE TABLE hob201934_kkt.u_text ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' AS
    SELECT content.userInn as user, content.dateTime.ddate as date, content.totalSum as sum FROM hob201934_kkt.raw WHERE subtype = "receipt";


FROM (
    SELECT * FROM hob201934_kkt.u_text
    WHERE user IS NOT NULL AND date IS NOT NULL AND sum IS NOT NULL
    DISTRIBUTE BY user SORT BY user, date
) sorted_uds
SELECT TRANSFORM (sorted_uds.user, sorted_uds.date, sorted_uds.sum)
--USING "/usr/bin/env python first_half_of_day.py" AS (user bigint)
USING "/usr/bin/env python first_half_of_day.py" AS (user string)
ORDER BY user
LIMIT 50;
