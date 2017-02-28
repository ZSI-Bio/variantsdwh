#!/bin/bash -x

for i in "$@"
do
case $i in
    -d=*|--dbname=*)
    DB_NAME="${i#*=}"
    ;;
    -m=*|--master=*)
    SPARK_MASTER="${i#*=}"
    ;;
    *)
            # unknown option
    ;;
esac
done


export HADOOP_USER_NAME=hdfs
HADOOP_CMD="hadoop fs"

#create
HDFS_TMP_DIR="/tmp/variantsdwh"
$HADOOP_CMD -mkdir -p $HDFS_TMP_DIR

DATA_DIR=data
#dims
wget -q "https://drive.google.com/uc?export=download&id=0B6Ngq87bjm-4c0hFZW9pdnZkdDQ" -O $DATA_DIR/dim_geography.gz

wget -q "https://drive.google.com/uc?export=download&id=0B6Ngq87bjm-4WnJGMVYzbm9MSU0" -O $DATA_DIR/dim_genomic_position_refseq.gz

wget -q "https://drive.google.com/uc?export=download&id=0B6Ngq87bjm-4NThxMTlqYllURkE" -O $DATA_DIR/dim_genomic_position_ensembl.gz

wget -q "https://drive.google.com/uc?export=download&id=0B6Ngq87bjm-4emtPLW1HSEN6U1U" -O $DATA_DIR/dim_disease.gz

cd $DATA_DIR
ls -1 *.gz | xargs -i $HADOOP_CMD -put -f {} $HDFS_TMP_DIR
cd ..

$HADOOP_CMD -ls  $HDFS_TMP_DIR | sed 's/\ \+/,/g' | cut -f8 -d',' | grep "^\/" | grep "gz$" |rev | cut -f1 -d'/' | rev | cut -f1 -d'.' | xargs -i spark-submit --master $SPARK_MASTER  --class pl.edu.pw.ii.zsibio.dwh.benchmark.DataLoader genomic-dwh-benchmark/target/scala-2.10/genomic-dwh-benchmark-assembly-1.0.jar --csvFile hdfs://${HDFS_TMP_DIR}/{}.gz --tableName {}_orc --dbName $DB_NAME --storageType orc

$HADOOP_CMD -ls  $HDFS_TMP_DIR | sed 's/\ \+/,/g' | cut -f8 -d',' | grep "^\/" | grep "gz$" |rev | cut -f1 -d'/' | rev | cut -f1 -d'.' | xargs -i spark-submit --master $SPARK_MASTER  --class pl.edu.pw.ii.zsibio.dwh.benchmark.DataLoader genomic-dwh-benchmark/target/scala-2.10/genomic-dwh-benchmark-assembly-1.0.jar --csvFile hdfs://${HDFS_TMP_DIR}/{}.gz --tableName {}_parquet --dbName $DB_NAME --storageType parquet


$HADOOP_CMD -ls  $HDFS_TMP_DIR | sed 's/\ \+/,/g' | cut -f8 -d',' | grep "^\/" | grep "gz$" |rev | cut -f1 -d'/' | rev | cut -f1 -d'.' | xargs -i spark-submit --master $SPARK_MASTER  --class pl.edu.pw.ii.zsibio.dwh.benchmark.DataLoader --verbose genomic-dwh-benchmark/target/scala-2.10/genomic-dwh-benchmark-assembly-1.0.jar --csvFile hdfs://${HDFS_TMP_DIR}/{}.gz --tableName {}_kudu --dbName $DB_NAME --storageType kudu


