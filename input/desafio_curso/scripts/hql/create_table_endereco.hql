DROP TABLE IF EXISTS ${TARGET_DATABASE}.${TARGET_TABLE_EXTERNAL};

CREATE EXTERNAL TABLE IF NOT EXISTS ${TARGET_DATABASE}.${TARGET_TABLE_EXTERNAL} ( 
   addressNumber string,
   city string, 
   country string,
   customerAddress1 string,
   customerAddress2 string,
   customerAddress3 string,
   CustomerAddress4 string,
   state string,
   zipCode string
)
COMMENT 'Tabela de ${TARGET_TABLE_EXTERNAL}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '${HDFS_DIR}'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS ${TARGET_DATABASE}.${TARGET_TABLE_GERENCIADA} (
   addressNumber string,
   city string, 
   country string,
   customerAddress1 string,
   customerAddress2 string,
   customerAddress3 string,
   CustomerAddress4 string,
   state string,
   zipCode string
)
PARTITIONED BY (DT_FOTO STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' 
TBLPROPERTIES ( 'orc.compress'='SNAPPY');

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE
   ${TARGET_DATABASE}.${TARGET_TABLE_GERENCIADA}
PARTITION(DT_FOTO) 
SELECT
   addressNumber string,
   city string, 
   country string,
   customerAddress1 string,
   customerAddress2 string,
   customerAddress3 string,
   CustomerAddress4 string,
   state string,
   zipCode string,
   ${PARTICAO} as DT_FOTO
FROM ${TARGET_DATABASE}.${TARGET_TABLE_EXTERNAL}
;