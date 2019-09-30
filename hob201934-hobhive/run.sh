#! /usr/bin/env bash

set -e
set -x

# hdfs dfs -cat /data/hive/fns/Document_1m.json
# {"fsId": "65549887878787988787878798988710",
#  "content": {"fiscalDriveNumber": "8710000100001103",
#              "modifiers": [{"discountSum": 18}],
#              "items": [{"price": 3790, "name": "Вода Святой Источник 0", "nds18": 578, "barcode": "0000000000000000", "sum": 3790, "quantity": 1},
#                        {"price": 5900, "name": "Сок SWELL Яблоко 0,25л", "barcode": "0000000000000000", "sum": 5900, "nds10": 536, "quantity": 1},
#                        {"price": 10900, "name": "Kофе Kапучино 200мл", "nds18": 3325, "barcode": "0000000000000000", "sum": 21800, "quantity": 2},
#                        {"price": 3790, "name": "АИ-95        N 4:06184", "nds18": 15257, "barcode": "0000000000000000", "sum": 100018, "quantity": 26.39}],
#              "operator": "KАССИР: Сапунова Е. В.",
#              "dateTime": {"$date": 1475565000000},
#              "operationType": 1,
#              "shiftNumber": 1,
#              "user": "АО РН-Москва",
#              "receiptCode": 3, "fiscalDocumentNumber": 111, "totalSum": 131490, "nds18": 19161,
#              "kktRegId": "878787878787878787438732321001965", "userInn": "54548743877698328787",
#              "cashTotalSum": 131490, "requestNumber": 109, "rawData": "7YjW11uk0dAOQbYQKLNO",
#              "taxationType": 1, "nds10": 536, "fiscalSign": "984321430195443321043", "ecashTotalSum": 0},
#  "ofdId": "ofd1", "subtype": "receipt", "documentId": 111,
#  "protocolSubversion": 1, "receiveDate": {"$date": 1475659657449},
#  "_id": {"$oid": "57f4c7892994015129de3663"}, "kktRegId": "878787878787878787438732321001965",
#  "protocolVersion": 2}

# format  time              cpu time
# parquet 34.257 seconds    10.160
# orc     33.144 seconds    13.820
# text    34.436 seconds    9.540

# hive
#~ hive -f stage4.sql > out
#~ echo "OUT"
#~ cat out

hive -f stage5.sql | tail -n +2
