#!/usr/bin/env python

from __future__ import print_function



import datetime
import sys
import json

last_user = None
user_is_fraud = False
last_kkt = None
last_kind = ""
last_t = None


def fflush_user():
    global user_is_fraud
    global last_user
    if last_user is not None:
        if user_is_fraud:
            print("{}".format(last_user))
        user_is_fraud = False


for line in sys.stdin:
    try:
        user, kkt, t, kind = line.strip().split("\t")
    except:
        j = json.loads(line)
        user = j["key"]["reducesinkkey0"]
        kkt = j["key"]["reducesinkkey1"]
        t = j["key"]["reducesinkkey2"]
        kind = j["value"]["_col0"]


    user = user.strip()
    kkt = kkt.strip()
    t = t.strip()
    kind = kind.strip()

    if last_user != user:
        fflush_user()
        last_user = user
        last_kkt = kkt

    if kkt != last_kkt:
        last_kind = ""
        last_kkt = kkt

    if (
        (kind == "receipt" and last_kind == "closeShift") or
        (kind == "openShift" and last_kind == "receipt")
    ) and last_t != t:
        user_is_fraud = True

    last_kind = kind
    last_t = t

fflush_user()

