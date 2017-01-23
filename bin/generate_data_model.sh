#!/bin/bash

ASSEMBLY_JAR=genomic-dwh-benchmark/target/scala-2.10/genomic-dwh-benchmark-assembly-1.0.jar
SQL_ROOT=sql/

#create Impala and Kudu data model
java -cp $ASSEMBLY_JAR  pl.edu.pw.ii.zsibio.dwh.benchmark.ExecuteStatement --useImpala --queryDir $SQL_ROOT/impala/ddl --storageType kudu

#create Hive data model using Parquet file format
java -cp $ASSEMBLY_JAR  pl.edu.pw.ii.zsibio.dwh.benchmark.ExecuteStatement --useHive --queryDir $SQL_ROOT/hive/ddl --storageType parquet

#create Hive data model using ORC file format
java -cp $ASSEMBLY_JAR  pl.edu.pw.ii.zsibio.dwh.benchmark.ExecuteStatement --useHive --queryDir $SQL_ROOT/hive/ddl --storageType orc

#load data into Hive tables


#load data into Kudu tables


#compute agregated tables in Hive


#compute agregated tables in Kudu

