#!/bin/bash -x
PWD=$(pwd | sed 's/bin//g' | sed 's/\/$//g')
DWH_TOOLS=`find ${PWD} -name genomic-dwh-benchmark-assembly*.jar`
java -cp $DWH_TOOLS pl.edu.pw.ii.zsibio.dwh.benchmark.ExecuteStatement --dbName dwh_dev --useHive --storageType orc --queryDir ../sql/hive/query --logFile results.log

