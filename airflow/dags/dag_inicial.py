from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Função simples para testar
def tarefa_exemplo():
    print("✅ DAG de teste executada com sucesso!")

# Definição da DAG
with DAG(
    dag_id="dag_teste",
    description="DAG de teste simples para verificar ambiente Airflow",
    schedule_interval="*/1 * * * *",  # Executa a cada minuto
    start_date=datetime(2023, 1, 1),
    catchup=False,  # Não executa tarefas passadas
    tags=["teste"],
) as dag:

    tarefa = PythonOperator(
        task_id="executar_teste",
        python_callable=tarefa_exemplo,
    )
