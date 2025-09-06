# Dockerfile.spark-master
FROM wlcamargo/spark-master

# Copiar requirements.txt
COPY requirements.txt /tmp/requirements.txt

# Instalar dependências Python
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Limpar arquivo temporário
RUN rm /tmp/requirements.txt