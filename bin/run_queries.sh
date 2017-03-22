#!/bin/bash -x

for i in "$@"
do
case $i in
    -d=*|--dbname=*)
    DB_NAME="${i#*=}"
    ;;
<<<<<<< HEAD
=======
    -i=*|--iter=*)
    ITER="${i#*=}"
    ;;
    -r=*|--dryRun=*)
    DR="${i#*=}"
    ;;
    -e=*|--engine=*)
    ENGINE="${i#*=}"
    ;;
     -p=*|--path=*)
    QUERY_DIR="${i#*=}"
    ;;
     -l=*|--log=*)
    LOG_PATH="${i#*=}"
    ;;
>>>>>>> 90efdeb1364415b0971d5a34d8d5beaa706692ec
    *)
            # unknown option
    ;;
esac
done


PWD=$(pwd | sed 's/bin//g' | sed 's/\/$//g')
DWH_TOOLS=`find ${PWD} -name genomic-dwh-benchmark-assembly*.jar`

CONF_FILE=$PWD/conf/application.conf

<<<<<<< HEAD
#orc
java -cp $DWH_TOOLS -Dconfig.file=${CONF_FILE} pl.edu.pw.ii.zsibio.dwh.benchmark.ExecuteStatement --dbName ${DB_NAME} --useHive --storageType orc --queryDir ../sql/hive/query --logFile results.log

#parquet

#kudu
=======
HADOOP_USER_NAME=hive

DRY_RUN=""

if [ "${DR}" = "yes" ]; then
DRY_RUN="--dryRun"
fi

for i in `seq 1 ${ITER}`;
do

if [ "${ENGINE}" = "hive" ]; then
#hive-orc
java -cp /etc/hive/conf/hive-site.xml:/mnt/hadoop/local/opt/cloudera/parcels/CDH-5.8.2-1.cdh5.8.2.p0.3/lib/hive/lib/hive-service-1.1.0-cdh5.8.2.jar:$DWH_TOOLS -Dconfig.file=${CONF_FILE} pl.edu.pw.ii.zsibio.dwh.benchmark.ExecuteStatement --dbName ${DB_NAME} --useHive --storageType orc --queryDir ${QUERY_DIR} --logFile ${LOG_PATH} ${DRY_RUN}

#hive-parquet
java -cp /etc/hive/conf/hive-site.xml:/mnt/hadoop/local/opt/cloudera/parcels/CDH-5.8.2-1.cdh5.8.2.p0.3/lib/hive/lib/hive-service-1.1.0-cdh5.8.2.jar:$DWH_TOOLS -Dconfig.file=${CONF_FILE} pl.edu.pw.ii.zsibio.dwh.benchmark.ExecuteStatement --dbName ${DB_NAME} --useHive --storageType parquet --queryDir ${QUERY_DIR} --logFile ${LOG_PATH} ${DRY_RUN}


elif [ "${ENGINE}" = "spark1" ]; then
#spark1-orc
java -cp /etc/hive/conf/hive-site.xml:/mnt/hadoop/local/opt/cloudera/parcels/CDH-5.8.2-1.cdh5.8.2.p0.3/lib/hive/lib/hive-service-1.1.0-cdh5.8.2.jar:$DWH_TOOLS -Dconfig.file=${CONF_FILE} pl.edu.pw.ii.zsibio.dwh.benchmark.ExecuteStatement --dbName ${DB_NAME} --useSpark1 --storageType orc --queryDir ${QUERY_DIR} --logFile ${LOG_PATH} ${DRY_RUN}

#spark1-parquet
java -cp /etc/hive/conf/hive-site.xml:/mnt/hadoop/local/opt/cloudera/parcels/CDH-5.8.2-1.cdh5.8.2.p0.3/lib/hive/lib/hive-service-1.1.0-cdh5.8.2.jar:$DWH_TOOLS -Dconfig.file=${CONF_FILE} pl.edu.pw.ii.zsibio.dwh.benchmark.ExecuteStatement --dbName ${DB_NAME} --useSpark1 --storageType parquet --queryDir ${QUERY_DIR} --logFile ${LOG_PATH} ${DRY_RUN}

elif [ "${ENGINE}" = "spark2" ]; then
#spark2-orc
java -cp /etc/hive/conf/hive-site.xml:/mnt/hadoop/local/opt/cloudera/parcels/CDH-5.8.2-1.cdh5.8.2.p0.3/lib/hive/lib/hive-service-1.1.0-cdh5.8.2.jar:$DWH_TOOLS -Dconfig.file=${CONF_FILE} pl.edu.pw.ii.zsibio.dwh.benchmark.ExecuteStatement --dbName ${DB_NAME} --useSpark2 --storageType orc --queryDir ${QUERY_DIR} --logFile ${LOG_PATH} ${DRY_RUN}

#spark2-parquet
java -cp /etc/hive/conf/hive-site.xml:/mnt/hadoop/local/opt/cloudera/parcels/CDH-5.8.2-1.cdh5.8.2.p0.3/lib/hive/lib/hive-service-1.1.0-cdh5.8.2.jar:$DWH_TOOLS -Dconfig.file=${CONF_FILE} pl.edu.pw.ii.zsibio.dwh.benchmark.ExecuteStatement --dbName ${DB_NAME} --useSpark2 --storageType parquet --queryDir ${QUERY_DIR} --logFile ${LOG_PATH} ${DRY_RUN}

elif [ "${ENGINE}" = "impala" ]; then
#impala-parquet
java -cp /etc/hive/conf/hive-site.xml:/mnt/hadoop/local/opt/cloudera/parcels/CDH-5.8.2-1.cdh5.8.2.p0.3/lib/hive/lib/hive-service-1.1.0-cdh5.8.2.jar:$DWH_TOOLS -Dconfig.file=${CONF_FILE} pl.edu.pw.ii.zsibio.dwh.benchmark.ExecuteStatement --dbName ${DB_NAME} --useImpala --storageType parquet --queryDir ${QUERY_DIR} --logFile ${LOG_PATH} ${DRY_RUN}

#impala-kudu
#java -cp /etc/hive/conf/hive-site.xml:/mnt/hadoop/local/opt/cloudera/parcels/CDH-5.8.2-1.cdh5.8.2.p0.3/lib/hive/lib/hive-service-1.1.0-cdh5.8.2.jar:$DWH_TOOLS -Dconfig.file=${CONF_FILE} pl.edu.pw.ii.zsibio.dwh.benchmark.ExecuteStatement --dbName ${DB_NAME} --useImpala --storageType kudu --queryDir ${QUERY_DIR} --logFile ${LOG_PATH} ${DRY_RUN}

fi

done

>>>>>>> 90efdeb1364415b0971d5a34d8d5beaa706692ec
