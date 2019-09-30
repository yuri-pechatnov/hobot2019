#! /usr/bin/env python

from __future__ import print_function

try:
    import sys
    import re

    reload(sys)
    sys.setdefaultencoding('utf-8') # required to convert to unicode

    #~ [2017-12-08.16:46:32] [Server thread/INFO]: Alex_5226 issued server command: /pardon avivzusim
    reg = re.compile(r".*\[(?P<date>\d\d\d\d\-\d\d\-\d\d)\.\d\d\:\d\d\:\d\d\]\s\[.*?\]\:\s(?P<name>.*?)\sissued\sserver\scommand\:\s(?P<cmd>.*?)\s")

    for line in sys.stdin:
        m = reg.match(line)
        if m is None:
            continue
        d = m.groupdict()
        print("{date}_{cmd}\t{date}\t{cmd}\t{count}".format(date=d["date"], cmd=d["cmd"], count=1))

except:
    pass
