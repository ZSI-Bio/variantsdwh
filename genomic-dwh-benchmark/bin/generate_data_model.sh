#!/bin/bash


#create Impala and Kudu data model
java -cp target/scala-2.10/genomic-dwh-benchmark-assembly-1.0.jar pl.edu.pw.ii.zsibio.dwh.benchmark.ExecuteStatement --useImpala --queryDir ../sql/impala/ddl --storageType kudu

#create Hive data model using Parquet file format
java -cp target/scala-2.10/genomic-dwh-benchmark-assembly-1.0.jar pl.edu.pw.ii.zsibio.dwh.benchmark.ExecuteStatement --useHive --queryDir ../sql/hive/ddl --storageType parquet

#create Hive data model using ORC file format
java -cp target/scala-2.10/genomic-dwh-benchmark-assembly-1.0.jar pl.edu.pw.ii.zsibio.dwh.benchmark.ExecuteStatement --useHive --queryDir ../sql/hive/ddl --storageType orc

#load data into Hive tables


#load data into Kudu tables


#compute agregated tables in Hive


#compute agregated tables in Kudu

