#!/bin/bash

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

ASSEMBLY_JAR=genomic-dwh-benchmark/target/scala-2.10/genomic-dwh-benchmark-assembly-1.0.jar
SQL_ROOT=sql/

#create Impala and Kudu data model
java -cp $ASSEMBLY_JAR  pl.edu.pw.ii.zsibio.dwh.benchmark.ExecuteStatement --useImpala --queryDir $SQL_ROOT/impala/ddl --storageType kudu --dbName ${DB_NAME}

#create Hive data model using Parquet file format
#java -cp $ASSEMBLY_JAR  pl.edu.pw.ii.zsibio.dwh.benchmark.ExecuteStatement --useHive --queryDir $SQL_ROOT/hive/ddl --storageType parquet --dbName ${DB_NAME}

#create Hive data model using ORC file format
#java -cp $ASSEMBLY_JAR  pl.edu.pw.ii.zsibio.dwh.benchmark.ExecuteStatement --useHive --queryDir $SQL_ROOT/hive/ddl --storageType orc --dbName ${DB_NAME}

#load data into Hive tables


#load data into Kudu tables


#compute agregated tables in Hive


#compute agregated tables in Kudu

