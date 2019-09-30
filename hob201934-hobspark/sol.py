# coding: utf-8

# In[1]:


# PYSPARK_PYTHON=/usr/bin/python3 spark2-submit --master "yarn" sol.py 2>err 1>out
# PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_PYTHON=/usr/bin/python3 PYSPARK_DRIVER_PYTHON_OPTS='notebook --ip="*" --port=30083 --NotebookApp.token="token" --no-browser' pyspark2 --master="yarn" --executor-memory=3GB



# In[16]:


import os, sys

from pyspark import SparkContext, SparkConf

is_jup = True
try:
    from IPython.display import display
    get_ipython()
except:
    is_jup = False
    def display(*a, **k):
        pass

if not is_jup:
    config = SparkConf().setAppName("my_super_app_pech2").setMaster("yarn")
    sc = SparkContext(conf=config)  # создаём контекст, пользуясь конфигом




local = (sc.master != "yarn") or True


# In[17]:


sc


# In[19]:


from pyspark.sql import SQLContext
sqlCtx = SQLContext(sc)
spark = sqlCtx.sparkSession


# In[20]:


def parse_edge(s):
    user, follower = s.split("\t")
    return (int(user), int(follower))

def step(item):
    prev_v, prev_d, next_v = item[0], item[1][0][0], item[1][1]
    return (next_v, (prev_d + 1, prev_v))

def complete(item):
    v, old_t, new_t = item[0], item[1][0], item[1][1]
    return (v, old_t if old_t is not None else new_t)

n = 3  # number of partitions

if local:
    efile = "/data/twitter/twitter_sample_small.txt"
else:
    efile = "/data/twitter/twitter_sample.txt"

from pyspark.sql.types import *

dfe = (spark.read
        .schema(StructType(fields=[
            StructField("to", LongType()),
            StructField("v", LongType())])
        )
        .format("csv")
        .option("sep", "\t")
        .load(efile))


# In[8]:


import pyspark.sql.functions as f

def step(item):
    prev_v, prev_d, next_v = item[0], item[1][0][0], item[1][1]
    return (next_v, (prev_d + 1, prev_v))

def complete(item):
    v, old_t, new_t = item[0], item[1][0], item[1][1]
    return (v, old_t if old_t is not None else new_t)

start = 12
x = start
target = 34
d = 0

distances = sqlCtx.createDataFrame([{"v": x, "d": 0, "p": -1}])
while True:
#     print(d)
    jc = distances.filter(distances.d == d).join(dfe, on="v", how="inner")
    distances.createOrReplaceTempView("jc_table")
    distances.createOrReplaceTempView("d_table")
    dfe.createOrReplaceTempView("e_table")
    candidates = spark.sql("""
SELECT e_table.to as v, min(d_table.d) + 1 as d, min(d_table.v) as p
FROM d_table
INNER JOIN e_table ON d_table.v = e_table.v
GROUP BY e_table.to
    """)

    distances.createOrReplaceTempView("d_table")
    candidates.createOrReplaceTempView("c_table")

    distances = spark.sql("""
SELECT
    CASE WHEN D.v IS NULL THEN C.v ELSE D.v END as v,
    CASE WHEN D.v IS NULL THEN C.d ELSE D.d END as d,
    CASE WHEN D.v IS NULL THEN C.p ELSE D.p END as p
FROM d_table as D
FULL OUTER JOIN c_table as C ON D.v = C.v"""
    ).persist()

#     distances.printSchema()
#     distances.show(4)

    found = distances.filter(distances.v == target).count()
    if found > 0:
#         print("FOUND", file=sys.stderr)
        break

    count = distances.filter(distances.d == d + 1).count()
    if count > 0:
        d += 1
    else:
#         print("NOT FOUND", file=sys.stderr)
        break


# In[ ]:





# In[9]:


distances = distances.cache()
v = target
ans = [target]
while v != start:
    v = distances.filter(distances.v == v).take(1)[0].p
    ans.append(v)



# In[10]:


ans_s = ",".join(map(str, ans[::-1]))
print(ans_s)


# In[22]:


if not is_jup:
    sys.exit(0)


get_ipython().system(u'jupyter nbconvert HW2sql-df.ipynb --to python --output sol')



# In[ ]:
