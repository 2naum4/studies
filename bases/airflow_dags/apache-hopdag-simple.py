from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from docker.types import Mount

default_args = {
    'owner': 'airflow',
    'description': 'sample-pipeline',
    'depend_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('sample-pipeline', default_args=default_args, schedule_interval=None, catchup=False, is_paused_upon_creation=False) as dag:
    start_dag = DummyOperator(task_id='start_dag')
    end_dag = DummyOperator(task_id='end_dag')
    hop = DockerOperator(task_id='sample-pipeline',
                         # use the Apache Hop Docker image. Add your tags here in the default apache/hop: syntax
                         image='apache/hop',
                         api_version='auto',
                         auto_remove=True,
                         environment={
                             'HOP_RUN_PARAMETERS': 'INPUT_DIR=',
                             'HOP_LOG_LEVEL': 'Basic',
                             'HOP_FILE_PATH': '${PROJECT_HOME}/transforms/null-if-basic.hpl',
                             'HOP_PROJECT_DIRECTORY': '/project',
                             'HOP_PROJECT_NAME': 'hop-airflow-sample',
                             'HOP_ENVIRONMENT_NAME': 'env-hop-airflow-sample.json',
                             'HOP_ENVIRONMENT_CONFIG_FILE_NAME_PATHS': '/project-config/envhop-airflow-sample.json',
                             'HOP_RUN_CONFIG': 'local'
                         },
                         docker_url="unix://var/run/docker.sock",
                         network_mode="bridge",
mounts=[Mount(target='/project', source='LOCAL_PATH_TO_PROJECT_FOLDER', type='bind'),
        Mount(target='/project-config', source='LOCAL_PATH_TO_ENV_FOLDER', type='bind')],
                         force_pull=False
                         )

start_dag >> hop >> end_dag
