#!/bin/bash -x

for i in "$@"
do
case $i in
    -d=*|--dbname=*)
    DB_NAME="${i#*=}"
    ;;
    *)
            # unknown option
    ;;
esac
done


PWD=$(pwd | sed 's/bin//g' | sed 's/\/$//g')
DWH_TOOLS=`find ${PWD} -name genomic-dwh-benchmark-assembly*.jar`

CONF_FILE=$PWD/conf/application.conf

#orc
java -cp $DWH_TOOLS -Dconfig.file=${CONF_FILE} pl.edu.pw.ii.zsibio.dwh.benchmark.ExecuteStatement --dbName ${DB_NAME} --useHive --storageType orc --queryDir ../sql/hive/query --logFile results.log

#parquet

#kudu
