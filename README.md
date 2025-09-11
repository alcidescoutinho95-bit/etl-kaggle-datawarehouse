## Transformação de scripts
Etapas para passar de .ipynb para .py, necessário rodar o airflow

Rodar no terminal a instalação do convert
```cmd
pip install nbconvert
pip install jupyter
```

utilizar o comando 
```cmd
-- Arquivo individual
python -m nbconvert src/Notebooks/caminho/arquivo.ipynb --to python --output-dir airflow/notebooks/

-- Todos arquivos dentro de uma pasta
nbconvert src/Notebooks/raw/*.ipynb --to python --output-dir airflow/notebooks/raw/
```

