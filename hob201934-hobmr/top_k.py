#! /usr/bin/env python

from __future__ import print_function

try:

    import sys
    import heapq

    import argparse

    reload(sys)
    sys.setdefaultencoding('utf-8') # required to convert to unicode

    parser = argparse.ArgumentParser(description='top k')
    parser.add_argument('-k', type=int)
    args = parser.parse_args()

    k = args.k

    top = heapq.nsmallest(k, sys.stdin, key=lambda x: x.split('\t')[0])

    for line in top:
        print(line, end="")

except:
    pass
