#!/usr/bin/env python

from __future__ import print_function



import datetime
import sys
import json

last_user = None
last_day = (0, -1)
best_day = (-1e100, 0)  # money, day

def fflush_day():
    global best_day
    global last_day
    best_day = max(best_day, last_day)


def fflush_user():
    global best_day
    global last_user
    fflush_day()
    if last_user is not None:
        print("{}\t{}\t{}".format(last_user, best_day[1], best_day[0]))
        best_day = (-1e100, 0)


for line in sys.stdin:
    # print(line.strip().split("\t"))
    try:
        user, t, money = line.strip().split("\t")
    except:
        j = json.loads(line)
        user = j["key"]["reducesinkkey0"]
        t = j["key"]["reducesinkkey1"]
        money = j["value"]["_col0"]
        # {"key":{"reducesinkkey0":"1001150441","reducesinkkey1":"1479913620000"},"value":{"_col0":11200}}

    day = datetime.datetime.utcfromtimestamp(int(t) / 1000).day
    money = float(money)
    if last_user != user:
        fflush_user()
        last_user = user
        last_day = (0, day)

    if day != last_day[1]:
        fflush_day()
        last_day = (0, day)

    last_day = (last_day[0] + money, day)

fflush_user()

