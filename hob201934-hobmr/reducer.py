#! /usr/bin/env python

from __future__ import print_function

try:
    import sys

    reload(sys)
    sys.setdefaultencoding('utf-8') # required to convert to unicode

    def read_input():
        for line in sys.stdin:
            key, date, cmd, count = line.strip().split('\t')
            yield (key, date, cmd, int(count))
        yield ("X", "", "", 0)

    last_params = None
    for key, date, cmd, count in read_input():
        if last_params is None:
            last_params = (key, date, cmd)
            last_count = 0
        if key != last_params[0]:
            new_key = "%s_%d" % (last_params[1], 10 ** 18 - 1 - last_count)
            print("%s\t%s\t%s\t%s" % (new_key, last_params[1], last_params[2], last_count))
            last_params = (key, date, cmd)
            last_count = 0
        last_count += count

except:
    pass
