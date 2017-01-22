#!/bin/bash -x


#create Impala and Kudu data model
java -cp target/scala-2.10/genomic-dwh-benchmark-assembly-1.0.jar pl.edu.pw.ii.zsibio.dwh.benchmark.CreateDataModelJob --help

#create Hive data model using Parquet file format


#create Hive data model using ORC file format


#load data into Hive tables


#load data into Kudu tables


#compute agregated tables in Hive


#compute agregated tables in Kudu

