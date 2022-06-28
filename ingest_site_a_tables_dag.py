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

with DAG(dag_id='Ingest_Henderson_tables', schedule_interval='*/10 * * * *', default_args=default_args, catchup=False) as dag:
    
    # Tasks dynamically generated
    tasks_1 = [BashOperator(task_id='Ingest_table_{0}'.format(t), bash_command='sleep 5'.format(t)) for t in  ["Losses","Action","Completeddatasheet","Completeddatasheetcomments","Meeting","Run","Shift"]]
    tasks_2 = [BashOperator(task_id='Validate_ingestion_{0}'.format(t), bash_command='sleep 5'.format(t)) for t in  ["Losses","Action","Completeddatasheet","Completeddatasheetcomments","Meeting","Run","Shift"]]

    task_4 = PythonOperator(task_id='Validate_ingestion', python_callable=process, op_args=['my super parameter'])

    task_5 = BashOperator(task_id='Send_notification', bash_command='echo "pipeline done"')

    tasks_1 >> task_4 >> tasks_2 >> task_5
        