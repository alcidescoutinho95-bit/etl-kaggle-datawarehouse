#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Adiciona a pasta raiz do projeto ao sys.path
import sys
import os
sys.path.append(os.path.abspath(os.path.join("..")))

## Importar a sessão spark criada no spark_session.py
from spark_session import spark
from minio import Minio
from datetime import datetime


# In[2]:


path = f"s3a://silver/geo_spatial_infos/{datetime.now().month}_{datetime.now().year}"
df = spark.read.format('parquet').load(path)
df_geospatial = df.select('latitude','longitude').distinct().toPandas()


# In[3]:


pip install numpy==1.26.4


# In[4]:


get_ipython().system('pip install geopandas')


# In[10]:


import tempfile
import requests
import zipfile
import geopandas as gpd
from shapely.geometry import Point
import json
from pyspark.sql import functions as f


# In[6]:


# Configura o cliente MinIO (mesmo de antes)
client = Minio(
    "minio:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False
)

# Dados do arquivo no MinIO
bucket = "raw"
minio_path = "geospatial/tl_2023_us_county.zip"

# Cria diretório temporário
with tempfile.TemporaryDirectory() as tmp_dir:
    zip_path = os.path.join(tmp_dir, "arquivo.zip")

    # 📥 Baixa o ZIP do MinIO
    client.fget_object(
        bucket,
        minio_path,
        zip_path
    )
    print(f"✅ Arquivo baixado do MinIO para: {zip_path}")

    # 📦 Extrai o conteúdo do ZIP
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(tmp_dir)
        print(f"📂 Arquivos extraídos em: {tmp_dir}")

    # 🔍 Localiza o .shp extraído
    shp_file = [f for f in os.listdir(tmp_dir) if f.endswith(".shp")]
    if not shp_file:
        raise FileNotFoundError("Nenhum arquivo .shp encontrado no ZIP extraído.")

    shp_path = os.path.join(tmp_dir, shp_file[0])
    print(f"📍 Shapefile localizado: {shp_path}")

    # 📚 Lê o shapefile com geopandas
    gdf = gpd.read_file(shp_path)
    print("✅ GeoDataFrame carregado com sucesso!")



# In[12]:


# Cria uma coluna de geometria a partir das coordenadas
geometry = [Point(xy) for xy in zip(df_geospatial['longitude'], df_geospatial['latitude'])]

# Cria o GeoDataFrame com CRS WGS84 (EPSG:4326), que é o padrão GPS
gdf_points = gpd.GeoDataFrame(df_geospatial.copy(), geometry=geometry, crs="EPSG:4326")

# Transforma para o mesmo CRS do GeoDataFrame do shapefile (gdf)
gdf_points = gdf_points.to_crs(gdf.crs)

# Faz o spatial join para agregar dados do shapefile aos pontos
gdf_joined = gpd.sjoin(gdf_points, gdf, how="left", predicate="within")

# Remove a coluna de geometria para voltar ao formato tabular
df_enriched = gdf_joined.drop(columns='geometry')

#  Seleciona as colunas que você quer no resultado final
df_final = df_enriched[['latitude', 'longitude', 'COUNTYFP', 'NAME', 'NAMELSAD']]


#  Converte o pandas DataFrame para JSON (orientação records - lista de dicts)
json_records = df_final.to_json(orient='records')

#  Transforma a string JSON em lista de dicionários Python
list_of_dicts = json.loads(json_records)

#  Cria o Spark DataFrame a partir da lista de dicionários
df_spark_enriched = spark.createDataFrame(list_of_dicts).select(
    'latitude', 'longitude',
    f.col('COUNTYFP').alias('county_code'),
    f.col('NAME').alias('county_name'),
    f.col('NAMELSAD').alias('county_full_name'),
)



# In[14]:


# Definição de como salvar os arquivos
bucket = 'silver'
project_name = 'geo_spatial_full'

save_path = f"s3a://{bucket}/{project_name}/{datetime.now().month}_{datetime.now().year}"

df_spark_enriched.write\
    .mode("overwrite") \
    .format('delta')\
    .save(save_path)


# In[15]:


get_ipython().system('pip install --upgrade numpy')

