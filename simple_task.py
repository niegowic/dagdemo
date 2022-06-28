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
    return 'Simple task done'

with DAG(dag_id='Simple_task', schedule_interval='*/45 * * * *', default_args=default_args, catchup=False) as dag:
    task_4 = PythonOperator(task_id='Validate_ingestion', python_callable=process, op_args=['my super parameter'])


    task_4