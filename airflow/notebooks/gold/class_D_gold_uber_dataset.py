#!/usr/bin/env python
# coding: utf-8

# ## Importação bibliotecas

# In[1]:


# Adiciona a pasta raiz do projeto ao sys.path
import sys
import os
sys.path.append(os.path.abspath(os.path.join("..")))

## Importar a sessão spark criada no spark_session.py
from spark_session import spark
from minio import Minio
from datetime import datetime


# ## Importação dos dados e junção

# In[2]:


base = 'D'


# In[3]:


path_kaggle = f"s3a://silver/uber_dataset/{datetime.now().month}_{datetime.now().year}/base_associada={base}"
df_kaggle = spark.read.format('delta').load(path_kaggle)
path_geo = f"s3a://silver/geo_spatial_full/{datetime.now().month}_{datetime.now().year}"
df_geo_spatial = spark.read.format('delta').load(path_geo)


# In[4]:


df_classe_A = df_kaggle.join(df_geo_spatial,
              ['latitude','longitude'],
              how='left')


# ## Escrever dados no Banco

# In[5]:


# Escrevendo no banco
df_classe_A.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/airflow") \
    .option("dbtable", f"gold.base_associada_{base}") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

