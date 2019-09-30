#!/usr/bin/env python

from __future__ import print_function



import datetime
import sys
import json

last_user = None
income = [0, 0, 0, 0] # count am, money am, count pm, money pm


def fflush_user():
    global income
    global last_user
    if last_user is not None:
        # print(last_user, income)
        if float(income[1]) / max(income[0], 1) > float(income[3]) / max(income[2], 1):
            print("{}".format(last_user))
        income = [0, 0, 0, 0]


for line in sys.stdin:
    try:
        user, t, money = line.strip().split("\t")
    except:
        j = json.loads(line)
        user = j["key"]["reducesinkkey0"]
        t = j["key"]["reducesinkkey1"]
        money = j["value"]["_col0"]
        # {"key":{"reducesinkkey0":"1001150441","reducesinkkey1":"1479913620000"},"value":{"_col0":11200}}

    dt = datetime.datetime.utcfromtimestamp(int(t) / 1000)
    day = dt.day
    is_pm = dt.hour >= 13
    money = float(money)

    if last_user != user:
        fflush_user()
        last_user = user

    if is_pm:
        income[2] += 1
        income[3] += money
    else:
        income[0] += 1
        income[1] += money


fflush_user()

