#!/usr/bin/env python

from __future__ import print_function

import sys

for line in sys.stdin:
    print("\t".join(e.strip() for e in line.split("\t")))
