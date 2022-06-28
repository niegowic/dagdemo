from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1),
    'owner': 'Airflow',
    'email': 'owner@test.com'
}

def process(p1):
    print(p1)
    return 'done'

with DAG(dag_id='Ingest_SiteA_tables', schedule_interval='*/15 * * * *', default_args=default_args, catchup=False) as dag:
    
    # Tasks dynamically generated 
    tasks_1 = [BashOperator(task_id='Ingest_table_{0}'.format(t), bash_command='sleep 1'.format(t)) for t in  range(20)]
    tasks_2 = [BashOperator(task_id='Validate_ingestion_{0}'.format(t), bash_command='sleep 1'.format(t)) for t in  range(20)]

    task_4 = PythonOperator(task_id='Validate_ingestion', python_callable=process, op_args=['my super parameter'])


    tasks_1 >> task_4 >> tasks_2