#! /usr/bin/env bash

# type your code here...

PYSPARK_PYTHON=/usr/bin/python3 spark2-submit --master "yarn" sol.py | tail -n 1
