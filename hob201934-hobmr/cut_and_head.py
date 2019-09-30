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
    i = 0
    for line in sys.stdin:
        if i < k:
            print(line[line.find('\t') + 1:], end="")
            i += 1

except:
    pass
