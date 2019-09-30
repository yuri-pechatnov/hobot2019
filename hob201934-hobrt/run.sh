#! /usr/bin/env bash

set -e
set -x


PYSPARK_PYTHON=/usr/bin/python3 spark2-submit --master "yarn" sol.py
