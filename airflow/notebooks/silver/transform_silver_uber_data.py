#!/usr/bin/env python
# coding: utf-8

# ## Importação de dados e bibliotecas

# In[1]:


# Adiciona a pasta raiz do projeto ao sys.path
import sys
import os
sys.path.append(os.path.abspath(os.path.join("..")))

## Importar a sessão spark criada no spark_session.py
from spark_session import spark
from pyspark.sql import functions as f
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from datetime import datetime
from minio import Minio


# In[2]:


# Listas todos os meses que devem ser extraídos os dados.
client = Minio(
    "minio:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False
)
print("✅ Acesso ao bucket do MinIO realizado com sucesso.")
bucket_name = "bronze"

objects = client.list_objects(bucket_name, recursive=True)
months_to_extract = list(set([obj.object_name.split('/')[2] for obj in objects]))
if len(months_to_extract) > 0:
    print("✅ Identificado meses presentes no bucket.")

# Ler cada arquivo e unir em um único dataframe
    df_union_all = None
    print(f"📥 Iniciando processo de ler os dados do bucket.")
    for fold in months_to_extract:
        ## Definição da path
        path = f"s3a://bronze/uber_dataset/{datetime.now().month}_{datetime.now().year}/{fold}"

        df_temp = spark.read.format('parquet').load(path)
        print(f"📄Dados referentes ao mês de {fold} transformado em Dataframe.")
        if df_union_all == None:
            df_union_all = df_temp
        else:
            df_union_all = df_union_all.unionAll(df_temp)
    print(f"✅ Dados importados e unidos.")
else:
    print(f"❌ Erro: na Leitura do bucket")



# ## Tratamento dos dados
# 
# abaixo utilizei duas abordagens diferentes, a primeira mais tradicional utilizando withcolumns, porém em questão performáticas ela acaba sendo não tão eficiênte no processamento desses dados, então abaixo demosntro a segunda opção que acelera o processamento desses dados

# In[3]:


# Criar uma função de criação de UDF 
def criar_udf_mapeamento(dicionario):
    return f.udf(lambda valor: dicionario.get(valor), StringType())

# Definir Mapas
days_of_week = {
    1: "Domingo",
    2: "Segunda-feira",
    3: "Terça-feira",
    4: "Quarta-feira",
    5: "Quinta-feira",
    6: "Sexta-feira",
    7: "Sábado",
}

base = {
    'B02512':'A',
    'B02598':'B',
    'B02682':'D',
    'B02617':'C',
    'B02764':'E',
}

# Criar UDFs para o de<>para necessário de dia da semana e Base
udf_dia = criar_udf_mapeamento(days_of_week)
udf_base = criar_udf_mapeamento(base)


# In[5]:


df_tradition = (
    df_union_all
    .withColumnRenamed('Lat','latitude')
    .withColumnRenamed('Lon','longitude')
    .withColumnRenamed('Base','base_associada')
    .withColumn('data',f.to_timestamp('Date/Time','M/d/yyyy H:mm:ss'))
    .withColumn('month',f.month('data'))
    .withColumn('year',f.year('data'))
    .withColumn('time',f.date_format('data', "HH:mm:ss"))
    .withColumn('week', udf_dia(f.dayofweek('data')))
    .withColumn('base_associada', udf_base('base_associada'))
    .withColumn('extraction_date', f.lit(datetime.now().date()))
).select('latitude','longitude','base_associada','month','year','time','week','extraction_date')


# In[4]:


df_performatic = df_union_all.select(
        f.col("Lat").alias("latitude"),
        f.col("Lon").alias("longitude"),
        f.col("Base").alias('base_associada'),
        f.to_timestamp("Date/Time", "M/d/yyyy H:mm:ss").alias("data")
).select(
        'latitude',
        'longitude',
        udf_base('base_associada').alias('base_associada'),
        f.month('data').alias("month"),
        f.year('data').alias("year"),
        f.date_format('data', "HH:mm:ss").alias("time"),
        udf_dia(f.dayofweek('data')).alias('week'),
        f.lit(datetime.now().date()).alias("extraction_date")
    )


# ## Exportar Dados

# In[5]:


# Definição de como salvar os arquivos
bucket = 'silver'
project_name = 'uber_dataset'

save_path = f"s3a://{bucket}/{project_name}/{datetime.now().month}_{datetime.now().year}"

df_performatic.write\
    .mode("overwrite") \
    .partitionBy("base_associada") \
    .format('delta')\
    .save(save_path)


# In[5]:


# Definição de como salvar os arquivos
bucket = 'silver'
project_name = 'geo_spatial_infos'

save_path = f"s3a://{bucket}/{project_name}/{datetime.now().month}_{datetime.now().year}"

df_performatic.write\
    .mode("overwrite") \
    .format('delta')\
    .save(save_path)

