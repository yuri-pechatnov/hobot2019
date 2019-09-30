#! /usr/bin/env bash

set -e
set -x

DST=hob201934@mipt-client.atp-fivt.org

ssh $DST "mkdir hobrt_hw || echo 'Directory exists'"

scp ./* $DST:~/hobrt_hw

ssh $DST "cd hobrt_hw ; chmod +x run.sh ; ./run.sh"
