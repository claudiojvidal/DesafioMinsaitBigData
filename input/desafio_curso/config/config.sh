#!/bin/bash
#caminho da pasta dos arquivos csv de entrada
CSV_DIR="input/desafio_curso/raw"
#caminho HDFS
HDFS_DIR="/datalake/raw"
#nome das tabelas
TABLES=("divisao" "regiao" "endereco" "clientes" "vendas")
TARGET_DATABASE="DESAFIO_CURSO"
#data atual no sistema
DATE="$(date --date="-0 day" "+%Y%m%d")"
PARTICAO="$(date --date="-0 day" "+%Y%m%d")"