
# coding: utf-8

# In[19]:


# -*- coding: utf-8 -*-

import pandas as pd
from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql import functions as f
from pyspark.sql.types import StringType, StructField, StructType
import os
import re

spark = SparkSession.builder.master("local[*]")    .enableHiveSupport()    .getOrCreate()

# Criando dataframes diretamente do Hive
df_clientes = spark.sql("SELECT * FROM DESAFIO_CURSO.tbl_clientes")
df_divisao = spark.sql("SELECT * FROM DESAFIO_CURSO.tbl_divisao")
df_endereco = spark.sql("SELECT * FROM DESAFIO_CURSO.tbl_endereco")
df_regiao = spark.sql("SELECT * FROM DESAFIO_CURSO.tbl_regiao")
df_vendas = spark.sql("SELECT * FROM DESAFIO_CURSO.tbl_vendas")


# In[20]:


#
# Espaço para tratar e juntar os campos e a criação do modelo dimensional
#

def replace_null_with_not_informed(df, fields):
    df_clean = df
    for field in fields:
        # Remover caracteres indesejados e espaços em branco
        #df_clean = df_clean.withColumn(field, regexp_replace(col(field), '[^\\w\\s]', ''))
        df_clean = df_clean.withColumn(field, regexp_replace(col(field), '[^\x20-\x7E]', ''))
        df_clean = df_clean.withColumn(field, trim(col(field)))
        # Substituir valores nulos e em branco por "Não informado"
        df_clean = df_clean.withColumn(field, when((col(field) == '') | (col(field) == 'NaN') | (col(field).isNull()), 'Não Informado').otherwise(col(field)))
    return df_clean

def replace_null_with_0(df, fields):
    df_clean = df
    for field in fields:
        # Remover caracteres indesejados e espaços em branco
        #df_clean = df_clean.withColumn(field, regexp_replace(col(field), '[^\\w\\s]', ''))
        df_clean = df_clean.withColumn(field, regexp_replace(col(field), '[^\x20-\x7E]', ''))
        df_clean = df_clean.withColumn(field, trim(col(field)))
        # Substituir valores nulos e em branco por 0
        df_clean = df_clean.withColumn(field, when((col(field) == '') | (col(field).isNull()), '0').otherwise(col(field)))
    return df_clean


# In[21]:


# Limpeza da tabela clientes

# Lista de campos para o tratamento de substituição de valores nulos
txt_fields_to_replace = ['addressnumber','businessfamily', 'customer',  'customerkey', 'customertype', 'division'
                         , 'lineofbusiness', 'phone', 'regioncode', 'regionalsalesmgr', 'searchtype']
# Aplicar a função para substituir valores nulos nos campos especificados
df_clientes_filled = replace_null_with_not_informed(df_clientes, txt_fields_to_replace)

# Lista de campos para o tratamento de substituição de valores nulos
num_fields_to_replace = ['businessunit']
# Aplicar a função para substituir valores nulos nos campos especificados
df_clientes_filled = replace_null_with_0(df_clientes_filled, num_fields_to_replace)
df_clientes_filled.show()

# Limpeza da tabela endereco

df_endereco.show()
txt_fields_to_replace = ['city', 'country', 'customeraddress1', 'customeraddress2', 'customeraddress3', 'customeraddress4', 'state', 'zipcode']
num_fields_to_replace = ['addressnumber']
df_endereco_filled = replace_null_with_not_informed(df_endereco, txt_fields_to_replace)
df_endereco_filled = replace_null_with_0(df_endereco_filled, num_fields_to_replace)
df_endereco_filled.show()

# Limpeza da tabela vendas

txt_fields_to_replace = ['customerkey', 'invoicenumber', 'itemclass','itemnumber', 'item', 'salesrep', 'um']
num_fields_to_replace = ['discountamount', 'linenumber', 'listprice', 'ordernumber', 'salesamount', 'salesamountbasedonlistprice', 'salescostamount', 'salesmarginamount'
                         , 'salesprice', 'salesquantity']
df_vendas_filled = replace_null_with_not_informed(df_vendas, txt_fields_to_replace)
df_vendas_filled = replace_null_with_0(df_vendas_filled, num_fields_to_replace)
df_vendas_filled.show()


# In[22]:


# criacao do modelo dimensional

df_clientes_filled.createOrReplaceTempView('clientes')
df_divisao.createOrReplaceTempView('divisao')
df_endereco_filled.createOrReplaceTempView('endereco')
df_regiao.createOrReplaceTempView('regiao')
df_vendas_filled.createOrReplaceTempView('vendas')

sql = '''
   SELECT
      v.actualdeliverydate AS actual_delivery_date,
      v.customerkey as customerKey,
      v.datekey as datekey,
      v.discountamount AS discount_amount,
      v.invoicedate AS invoice_date,
      v.invoicenumber AS invoice_number,
      v.itemclass AS item_class,
      v.itemnumber AS item_number,
      v.item as item,
      v.linenumber AS line_number,
      v.listprice AS list_price,
      v.ordernumber AS order_number,
      v.promiseddeliverydate AS promised_delivery_date,
      v.salesamount AS sales_amount,
      v.salesamountbasedonlistprice AS sales_amount_based_on_list_price,
      v.salescostamount AS sales_cost_amount,
      v.salesmarginamount AS sales_margin_amount,
      v.salesprice AS sales_price,
      v.salesquantity AS sales_quantity,
      v.salesrep AS sales_rep,
      v.um AS u_m,
      c.addressnumber as address_number,
      c.businessfamily as business_family,
      c.businessunit as business_unit,
      c.customer as customer,
      c.customertype as customer_type,
      c.division as division,
      c.lineofbusiness as line_of_business,
      c.phone as phone,
      c.regioncode as region_code,
      c.regionalsalesmgr as regional_sales_Mgr,
      c.searchtype as search_type,
      e.city,
      e.country,
      e.customeraddress1 as customer_address_1,
      e.customeraddress2 as customer_address_2,
      e.customeraddress3 as customer_address_3,
      e.customeraddress4 as customer_address_4,
      e.state as state,
      e.zipcode as zip_code,
      d.divisionname as division_name,
      r.regionname as region_name
   FROM vendas AS v
   INNER JOIN clientes AS c ON c.customerkey = v.customerkey
   LEFT JOIN endereco AS e ON c.addressnumber = e.addressnumber
   LEFT JOIN divisao AS d ON c.division = d.division
   LEFT JOIN regiao AS r ON c.regioncode = r.regioncode
'''

# Criação da STAGE
df_stage = spark.sql(sql)

# Criação dos Campos Calendario
df_stage = (df_stage
            .withColumn('ano', year(to_date(df_stage.invoice_date,'dd/MM/yyyy')))
            .withColumn('mes', month(to_date(df_stage.invoice_date,'dd/MM/yyyy')))
            .withColumn('dia', dayofmonth(to_date(df_stage.invoice_date,'dd/MM/yyyy')))
            .withColumn('trimestre', quarter(to_date(df_stage.invoice_date,'dd/MM/yyyy')))
           )

# Criação das Chaves do Modelo

df_stage = df_stage.withColumn("DW_CLIENTE", sha2(concat_ws("", df_stage.customerKey, df_stage.customer, df_stage.address_number, df_stage.division, df_stage.region_code), 256))
df_stage = df_stage.withColumn("DW_DIVISAO", sha2(concat_ws("", df_stage.division, df_stage.division_name), 256))
df_stage = df_stage.withColumn("DW_ENDERECO", sha2(concat_ws("", df_stage.address_number, df_stage.city, df_stage.country, df_stage.state), 256))
df_stage = df_stage.withColumn("DW_REGIAO", sha2(concat_ws("", df_stage.region_code, df_stage.region_name), 256))
df_stage = df_stage.withColumn("DW_TEMPO", sha2(concat_ws("", df_stage.invoice_date, df_stage.ano, df_stage.mes, df_stage.dia), 256))

df_stage.createOrReplaceTempView('stage')

df_stage.show()


# In[23]:


# Como vi mais a frente um problema da criacao da fato faço aqui estas consultas  

from pyspark.sql.functions import isnan
df_filtered = df_stage.filter(col("sales_amount").isNull())
df_filtered.show()
df_filtered = df_stage.filter(isnan(col("sales_amount").cast("decimal")))
df_filtered.show()


# In[24]:


#Criando a dimensão Clientes

dim_clientes = spark.sql('''
    SELECT DISTINCT
        DW_CLIENTE,
        customerKey,
        customer,
        address_number,
        business_family,
        business_unit,
        customer_type,
        division,
        line_of_business,
        phone,
        region_code,
        regional_sales_Mgr,
        search_type
    FROM stage
''')

#Criando a dimensão Divisao

dim_divisao = spark.sql('''
    SELECT DISTINCT
        DW_DIVISAO,
        division,
        division_name
    FROM stage
''')

#Criando a dimensão Endereco

dim_endereco = spark.sql('''
    SELECT DISTINCT
        DW_ENDERECO,
        address_number,
        city,
        country,
        customer_address_1,
        customer_address_2,
        customer_address_3,
        customer_address_4,
        state,
        zip_code
    FROM stage
''')

#Criando a dimensão Regiao

dim_regiao = spark.sql('''
    SELECT DISTINCT
        DW_REGIAO,
        region_code,
        region_name
    FROM stage
''')

#Criando a dimensão Tempo

dim_tempo = spark.sql('''
    SELECT DISTINCT
        DW_TEMPO,
        invoice_date,
        ano,
        mes,
        dia
    FROM stage
''')

dim_clientes.show()
dim_divisao.show()
dim_endereco.show()
dim_regiao.show()
dim_tempo.show()


# In[25]:


#Criando a Fato vendas

ft_vendas = spark.sql('''
    SELECT
        DW_CLIENTE,
        DW_DIVISAO,
        DW_ENDERECO,
        DW_REGIAO,
        DW_TEMPO,
        sum(sales_amount) as valor_de_Venda
    FROM stage
    group by
        DW_CLIENTE,
        DW_DIVISAO,
        DW_ENDERECO,
        DW_REGIAO,
        DW_TEMPO
''')

ft_vendas.show()


# In[26]:


# função para salvar os dados

def salvar_df(df, file):
   output = "/input/desafio_curso/gold/" + file
   erase = "hdfs dfs -rm " + output + "/*"
   rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* /input/desafio_curso/gold/"+file+".csv"
   print(rename)
   df.coalesce(1).write      .format("csv")      .option("header", True)      .option("delimiter", ";")      .mode("overwrite")      .save("/datalake/gold/"+file+"/")
   os.system(erase)
   os.system(rename)

# salvando as tabelas de dimensão e fato em GOLD
salvar_df(ft_vendas, 'ft_vendas')
salvar_df(dim_clientes, 'dim_clientes')
salvar_df(dim_divisao, 'dim_divisao')
salvar_df(dim_endereco, 'dim_endereco')
salvar_df(dim_regiao, 'dim_regiao')
salvar_df(dim_tempo, 'dim_tempo')

