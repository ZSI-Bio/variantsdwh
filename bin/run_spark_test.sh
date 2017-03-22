#!/bin/bash -x

for i in "$@"
do
case $i in
    -d=*|--dbname=*)
    DB_NAME="${i#*=}"
    ;;
    -i=*|--iter=*)
    ITER="${i#*=}"
    ;;
    -t==*|--querytype=*)
    QUERY_TYPE="${i#*=}"
    ;;
    *)
            # unknown option
    ;;
esac
done

PARENT_QUERY_DIR=/mnt/zgmvol/_forge/zsibio/dwh2_tests/query
RESULTS_DIR=/mnt/zgmvol/_forge/zsibio/dwh2_tests/results

RAW_DIR=${PARENT_QUERY_DIR}/raw
AGGR_DIR=${PARENT_QUERY_DIR}/aggr
FACT_ONLY_DIR=${PARENT_QUERY_DIR}/fact_only

SPARK1_SBIN_DIR=/mnt/hadoop/local/opt/spark-1.6.3/sbin
SPARK2_SBIN_DIR=/mnt/hadoop/local/opt/spark-2.1.0-bin-hadoop2.6/sbin

SPARK_PARAMS="--master yarn-client --executor-memory 4g --num-executors 160  --hiveconf hive.server2.thrift.port=12000"



${SPARK1_SBIN_DIR}/stop-thriftserver.sh
${SPARK2_SBIN_DIR}/stop-thriftserver.sh
${SPARK1_SBIN_DIR}/start-thriftserver.sh ${SPARK_PARAMS}

sleep 60
#spark1
./run_queries.sh --dbname=${DB_NAME} --iter=${ITER}  --engine=spark1 --dryRun=no --path=${PARENT_QUERY_DIR}/${QUERY_TYPE} --log=${RESULTS_DIR}/results_${QUERY_TYPE}.csv

${SPARK1_SBIN_DIR}/stop-thriftserver.sh
${SPARK2_SBIN_DIR}/start-thriftserver.sh ${SPARK_PARAMS}

sleep 60
#spark2
./run_queries.sh --dbname=${DB_NAME} --iter=${ITER}  --engine=spark2 --dryRun=no --path=${PARENT_QUERY_DIR}/${QUERY_TYPE} --log=${RESULTS_DIR}/results_${QUERY_TYPE}.csv

${SPARK2_SBIN_DIR}/stop-thriftserver.sh




