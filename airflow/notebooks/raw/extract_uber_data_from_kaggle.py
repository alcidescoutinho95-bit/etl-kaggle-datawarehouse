#!/usr/bin/env python
# coding: utf-8

# # Kaggle

# In[1]:


from datetime import datetime
import os
from kaggle.api.kaggle_api_extended import KaggleApi
import time 


# In[2]:


def LeituraAPI(folder_name: str, source: str, bucket_name: str = "raw"):
    start = time.time()
    try:
        # 📌 Autenticação Kaggle
        os.environ['KAGGLE_CONFIG_DIR'] = '/home/user/.kaggle'
        api = KaggleApi()
        api.authenticate()
        print("✅ Autenticação Kaggle bem-sucedida")

        # 🗂️ Diretório local de destino com versionamento por mês/ano
        now_str = f"{datetime.now().month}_{datetime.now().year}"
        path = f"/home/user/datasets/{folder_name}/{now_str}"
        print(f"📁 Diretório local de destino: {path}")
        os.makedirs(path, exist_ok=True)

        # 📥 Download do dataset do Kaggle e extração
        print(f"⬇️ Baixando dataset '{source}' para '{path}'...")
        api.dataset_download_files(source, path=path, unzip=True)

        # 📂 Lista os arquivos baixados
        arquivos = os.listdir(path)
        print(f"📄 Arquivos salvos localmente:")
        for arq in arquivos:
            print(f" - {arq}")
        print(f"\nTotal: {len(arquivos)} arquivo(s)\n")
        print(f"💾 Salvo localmente em: {path}")
        end = time.time()
        print(f"⏱️ Processo de acesso a API e salvamento dos dados em diretório local levou {end - start:.2f} segundos.")
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()


# In[3]:


folder_name = 'uber_dataset'
source = 'fivethirtyeight/uber-pickups-in-new-york-city'
# bucket_name = 'raw'
LeituraAPI(folder_name=folder_name, source=source)

