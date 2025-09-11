#!/usr/bin/env python
# coding: utf-8

# # Import libraries and create spark session

# In[1]:


# Adiciona a pasta raiz do projeto ao sys.path
import sys
import os
sys.path.append(os.path.abspath(os.path.join("..")))

## Importar a sessão spark criada no spark_session.py
from spark_session import spark
from datetime import datetime
from minio import Minio
import tempfile
import time


# In[2]:


def DataFrameGenerator(folder_name: str, bucket: str, project_name: str):
    start = time.time()
    try:
        path = f'/home/user/datasets/{folder_name}/{datetime.now().month}_{datetime.now().year}'
        files = os.listdir(path)
        print(f"📁 Diretório local de busca: {path}")
        files_return = [file for file in files if file.endswith('14.csv')]
        print(f"📄 Um total de {len(files_return)} arquivos foram encontrados para os filtros aplicados.")


        # Save delta section 
        print(f"📥 Iniciando processo de salvar dados em Delta.")
        delta_save_path = f"s3a://{bucket}/{project_name}/{datetime.now().month}_{datetime.now().year}"
        for csv_file in files_return:
            print(f"⬆️ Enviando {csv_file} no formato Delta para MinIO...")
            path_csv = f"{path}/{csv_file}"
            folder_nome_for_csv = csv_file.split('-')[-1].replace('14.csv','')
            delta_save_path = f"s3a://{bucket}/{project_name}/{datetime.now().month}_{datetime.now().year}/{folder_nome_for_csv}"
            spark.read.csv(path_csv,header=True,inferSchema=True)\
            .write.format("parquet").mode("append").save(delta_save_path)

        print(f"✅💾 Arquivos salvos no MinIO")
        end = time.time()
        print(f"⏱️ Processou levou um total de {end - start:.2f} segundos.")

    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()


# In[3]:


folder_name = 'uber_dataset'
DataFrameGenerator(folder_name, bucket='bronze', project_name='uber_dataset')

