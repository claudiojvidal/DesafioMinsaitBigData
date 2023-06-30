#!/bin/bash
BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONFIG="${BASEDIR}/../../config/config.sh"
source $CONFIG
# Criação das pastas e envio dos arquivos ao HDFS
echo "Exportação de arquivos ao HDFS"
docker exec -it namenode hdfs dfs -mkdir -p $HDFS_DIR
docker exec -it namenode hdfs dfs -chmod 777 $HDFS_DIR
for table in "${TABLES[@]}"
do
   echo "Envio da tabela $table."
   docker exec -it namenode hdfs dfs -mkdir -p $HDFS_DIR/$table
   docker exec -it namenode hdfs dfs -chmod 777 $HDFS_DIR/$table
   docker exec -it namenode hdfs dfs -copyFromLocal $CSV_DIR/$table.csv $HDFS_DIR/$table
   docker exec -it namenode hdfs dfs -chmod 777 $HDFS_DIR/$table/$table.csv
   echo "Envio da tabela $table finalizado."
done
echo "Exportação de arquivos ao HDFS em ${DATE} finalizada."
