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




#presto
./run_queries.sh --dbname=${DB_NAME} --iter=${ITER}  --engine=impala --dryRun=no --path=${PARENT_QUERY_DIR}/${QUERY_TYPE} --log=${RESULTS_DIR}/results_${QUERY_TYPE}_impala.csv

