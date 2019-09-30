add jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;
ADD FILE ./best_day.py;
ADD FILE ./cat.py;
ADD FILE ./first_half_of_day.py;
ADD FILE ./frauds.py;
ADD FILE ./strip_all.py;

SET hive.cli.print.header=true;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;




FROM (
--    SELECT * FROM hob201934_kkt.u_text
    FROM (
        FROM hob201934_kkt.u_text as uu
        SELECT TRANSFORM (uu.user, uu.kkt, uu.date, uu.subtype)
        USING "/usr/bin/env python strip_all.py" AS (user, kkt, date, subtype)
    ) uu
    SELECT *
    WHERE user IS NOT NULL AND kkt IS NOT NULL AND date IS NOT NULL
    DISTRIBUTE BY user SORT BY user, kkt, date
) sorted_uds
SELECT TRANSFORM (sorted_uds.user, sorted_uds.kkt, sorted_uds.date, sorted_uds.subtype)
--USING "/usr/bin/env python first_half_of_day.py" AS (user bigint)
USING "/usr/bin/env python frauds.py" AS (user string)
ORDER BY user
LIMIT 50;
