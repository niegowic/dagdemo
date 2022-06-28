from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'custom_key1': 'custom_value1',
    'custom_key2': 'custom_value2'
}

dag = DAG(
    'simple_DAG_with_params',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval='* */3 * * *',
    user_defined_macros=default_args ,
)

bash_command = """
    echo "access via DAG's user_defined_macros = {{ custom_key1 }}"
    echo "access via Operator's params = {{ params.custom_key2 }}"
"""

t1 = BashOperator(
    task_id='print_in_bash_op',
    bash_command=bash_command,
    params=default_args,
    dag=dag,
)

def myfunc(**context):
    print(context['templates_dict']['custom_key1'])
    print(context['templates_dict']['custom_key2'])


t2 = PythonOperator(
    task_id='print_in_python_op',
    python_callable=myfunc, 
    templates_dict=default_args,
    provide_context=True,
    dag=dag,
)

templates_dict={
    'custom_key1': '{{ custom_key1 }}',
    'custom_key2': '{{ custom_key2 }}'
}

t3 = PythonOperator(
    task_id='print_in_python_op_2',
    python_callable=myfunc, 
    templates_dict=templates_dict,
    provide_context=True,
    dag=dag,
)

t1 >> t2 >> t3
