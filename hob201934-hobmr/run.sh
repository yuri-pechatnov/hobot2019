#! /usr/bin/env bash

# type your code here...

# cat 001 | ./mapper.py  | ./reducer.py  | sort -k1,1 -k3rn

set -e
set -x

OUT_DIR="hob201934hobmr_cmd_top_"$(date +"%s%6N")
NUM_REDUCERS=4
TOP_K=10



hdfs dfs -rm -r -f -skipTrash ${OUT_DIR}.1 > /dev/null
hdfs dfs -rm -r -f -skipTrash ${OUT_DIR} > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.name="Streaming cmd count" \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -D mapreduce.partition.keycomparator.options='-k1' \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
    -files mapper.py,reducer.py \
    -mapper "python mapper.py" \
    -reducer "python reducer.py" \
    -input /data/minecraft-server-logs/* \
    -output ${OUT_DIR}.1 > /dev/null

# hdfs dfs -cat ${OUT_DIR}.1/part-* | sort -k1 1>&2

# take top $TOP_K
# output of every reducer is no more than $TOP_K rows
# so no more that partitions_count * $TOP_K comes to reducer
# notice: sorting of big data is made by hadoop
# I only take top $TOP_K and sort small data
yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.name="Streaming cmd count" \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.partition.keycomparator.options='-k1' \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
    -files top_k.py,cut_and_head.py \
    -mapper "python top_k.py -k $TOP_K" \
    -reducer "python cut_and_head.py -k $TOP_K" \
    -input ${OUT_DIR}.1/* \
    -output ${OUT_DIR} > /dev/null

hdfs dfs -rm -r -f -skipTrash ${OUT_DIR}.1 > /dev/null

hdfs dfs -ls ${OUT_DIR} 1>&2
hdfs dfs -cat ${OUT_DIR}/part-* 1>&2
hdfs dfs -cat ${OUT_DIR}/part-*

hdfs dfs -rm -r -f -skipTrash ${OUT_DIR}.1 > /dev/null
hdfs dfs -rm -r -f -skipTrash ${OUT_DIR} > /dev/null
