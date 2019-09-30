#! /usr/bin/env bash

set -e
set -x

DST=hob201934@mipt-client.atp-fivt.org

ssh $DST "mkdir hobhive_hw || echo 'Directory exists'"

scp ./* $DST:~/hobhive_hw

ssh $DST "cd hobhive_hw ; chmod +x run.sh ; ./run.sh"
