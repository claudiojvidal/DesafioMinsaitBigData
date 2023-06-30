DROP TABLE IF EXISTS ${TARGET_DATABASE}.${TARGET_TABLE_EXTERNAL};

CREATE EXTERNAL TABLE IF NOT EXISTS ${TARGET_DATABASE}.${TARGET_TABLE_EXTERNAL} ( 
   actualDeliveryDate  string,
   customerKey string,
   dateKey string,
   discountAmount string,
   invoiceDate string,
   invoiceNumber string,
   itemClass string,
   itemNumber string,
   item string,
   lineNumber string,
   listPrice string,
   orderNumber string,
   promisedDeliveryDate string,
   salesAmount string,
   salesAmountBasedOnListPrice string,
   salesCostAmount string,
   salesMarginAmount string,
   salesPrice string,
   salesQuantity string,
   salesRep string,
   uM string
)
COMMENT 'Tabela de ${TARGET_TABLE_EXTERNAL}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '${HDFS_DIR}'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS ${TARGET_DATABASE}.${TARGET_TABLE_GERENCIADA} (
   actualDeliveryDate  string,
   customerKey string,
   dateKey string,
   discountAmount string,
   invoiceDate string,
   invoiceNumber string,
   itemClass string,
   itemNumber string,
   item string,
   lineNumber string,
   listPrice string,
   orderNumber string,
   promisedDeliveryDate string,
   salesAmount string,
   salesAmountBasedOnListPrice string,
   salesCostAmount string,
   salesMarginAmount string,
   salesPrice string,
   salesQuantity string,
   salesRep string,
   uM string
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
   actualDeliveryDate  string,
   customerKey string,
   dateKey string,
   discountAmount string,
   invoiceDate string,
   invoiceNumber string,
   itemClass string,
   itemNumber string,
   item string,
   lineNumber string,
   listPrice string,
   orderNumber string,
   promisedDeliveryDate string,
   salesAmount string,
   salesAmountBasedOnListPrice string,
   salesCostAmount string,
   salesMarginAmount string,
   salesPrice string,
   salesQuantity string,
   salesRep string,
   uM string,
   ${PARTICAO} as DT_FOTO
FROM ${TARGET_DATABASE}.${TARGET_TABLE_EXTERNAL}
;