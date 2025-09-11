#!/usr/bin/env python
# coding: utf-8

# In[3]:


from minio import Minio
import tempfile
import zipfile
import os
import requests


# In[4]:


url = "https://www2.census.gov/geo/tiger/TIGER2023/COUNTY/tl_2023_us_county.zip"

client = Minio(
    "minio:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False
)
bucket = "raw"
minio_path = "geospatial/tl_2023_us_county.zip"

# Faz o download e envia para o MinIO
with tempfile.TemporaryDirectory() as tmp_dir:
    local_path = os.path.join(tmp_dir, "shapefile.zip")

    # Baixa da URL
    print(f"🔽 Baixando arquivo: {url}")
    response = requests.get(url)
    response.raise_for_status()
    with open(local_path, "wb") as f:
        f.write(response.content)

    print(f"📁 Arquivo salvo temporariamente em: {local_path}")

    # Envia para MinIO
    client.fput_object(bucket, minio_path, local_path)
    print(f"✅ Enviado para MinIO: s3://{bucket}/{minio_path}")

