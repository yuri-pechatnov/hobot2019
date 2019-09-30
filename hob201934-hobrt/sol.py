
# coding: utf-8

# In[1]:


import os
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

import hyperloglog
from concurrent.futures import Future

from hdfs import Config
import subprocess

try:
    client = Config().get_client()
except:
    config_fname = "hdfscli.cfg"
    with open(config_fname, "wt") as f:
        f.write("""
[global]
default.alias = default

[default.alias]
url = http://mipt-master.atp-fivt.org:50070
user = {user}
        """.format(user=os.environ["USER"]))
    client = Config(config_fname).get_client()


nn_address = subprocess.check_output('hdfs getconf -confKey dfs.namenode.http-address', shell=True).strip().decode("utf-8")
conf = SparkConf().setMaster('yarn-client')
sc = SparkContext.getOrCreate(conf)  # т.к. работаем с HDFS, можем запустить Spark в распределённом режиме


# In[ ]:





# In[2]:


def extract_segments(line):
    user_id, ua = line.strip().split("\t")
    segments = []
    if ua.find("iPhone") != -1:
        segments.append("seg_iphone")
    if ua.find("Firefox") != -1:
        segments.append("seg_firefox")
    if ua.find("Windows") != -1:
        segments.append("seg_windows")

    return [(s, user_id) for s in segments]


def update_count(values, old):
    old = old or hyperloglog.HyperLogLog(0.01)
    for v in values:
        old.add(v)
    return old


finished = Future()

def recognize_finish(rdd):
    global finished
    if rdd.isEmpty():
        finished.set_result(True)

top_all = None

def print_rdd(rdd):
    global top_all
    top_all = rdd.take(10)  # 3 in fact
#     for row in top_all:
#          print('{}\t{}'.format(*row))


# In[3]:


# Эмулируем реальную жизнь, когда данные поступают частями с периодичностью
DATA_PATH = "/data/course4/uid_ua_100k_splitted_by_5k"
batches = [sc.textFile(os.path.join(*[nn_address, DATA_PATH, path])) for path in client.list(DATA_PATH)]  # формируем батчи из файлов датасета
#batches = batches[:2]
BATCH_TIMEOUT = 1 # раз в 5 с. посылаем батчи в виде RDD
ssc = StreamingContext(sc, BATCH_TIMEOUT)
ssc.checkpoint("./checkpoints")


dstream = ssc.queueStream(rdds=batches)

result = (dstream
    .flatMap(extract_segments)
)
#result.foreachRDD(print_rdd)
result.foreachRDD(recognize_finish)
(result
    .updateStateByKey(update_count)
    .map(lambda x: (x[0], len(x[1])))
    #.foreachRDD(print_rdd)
    .foreachRDD(lambda rdd: print_rdd(rdd.sortBy(lambda x: -x[1])))
)

ssc.start()
finished.result()
ssc.stop()



# In[4]:


for row in top_all:
    print('{}\t{}'.format(*row))


# In[ ]:





# In[ ]:
