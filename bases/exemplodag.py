# ***************************************************************
# DAG Exemplo
# ***************************************************************
# Importando as bibliotecas
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
# Parametros basicos
default_args = {
   'owner': 'airflow',
   'depends_on_past': False,
   'start_date': datetime(2019, 11, 29)
   }
# DAG
with DAG(
    'dag_exemplo',
    schedule_interval='0 23 * * *',
    catchup=True,
    default_args=default_args
    ) as dag:
# Tarefas da DAG
    t0 = DummyOperator(
        task_id='Start')
    t1 = BashOperator(
        task_id='Task_1',
        bash_command="""
        echo 1
        """)
    t2 = BashOperator(
        task_id='Task_2',
        bash_command="""
        echo 2
        """)
    tw = BashOperator(
        task_id='Wait',
        bash_command='sleep 50s')
    t3 = BashOperator(
        task_id='Task_3',
        bash_command="""
        echo 3
        """)

# Ordem das tarefas
t0 >> t1 >> t2
t1 >> tw >> t3