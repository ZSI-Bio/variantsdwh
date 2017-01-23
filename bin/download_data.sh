#!/bin/bash -x


export HADOOP_USER_NAME=hdfs
HADOOP_CMD="hadoop fs"

#create
HDFS_TMP_DIR="/tmp/variantsdwh"
$HADOOP_CMD -mkdir -p $HDFS_TMP_DIR

DATA_DIR=data
#dims
wget "https://drive.google.com/uc?export=download&id=0B6Ngq87bjm-4c0hFZW9pdnZkdDQ" -O $DATA_DIR/dim_geography.gz

wget "https://drive.google.com/uc?export=download&id=0B6Ngq87bjm-4WnJGMVYzbm9MSU0" -O $DATA_DIR/dim_genomic_position_refseq.gz

wget "https://drive.google.com/uc?export=download&id=0B6Ngq87bjm-4NThxMTlqYllURkE" -O $DATA_DIR/dim_genomic_position_ensembl.gz

wget "https://drive.google.com/uc?export=download&id=0B6Ngq87bjm-4emtPLW1HSEN6U1U" -O $DATA_DIR/dim_disease.gz

cd $DATA_DIR
ls -1 *.gz | xargs -i $HADOOP_CMD -put {} $HDFS_TMP_DIR  
