from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'Higor Souza',
    "depends_on_past": False,
    "start_date": datetime(2021, 11, 20, 19),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "Titanic_1",
    description="Carrega dados acerca dos passageiros a bordo do Titanic",
    default_args=default_args,
    schedule_interval=timedelta(minutes=3)
)

## Definição de funções para os operators Python

def sort_h_m():
    df = pd.read_csv('/usr/local/airflow/data/titanic.csv')
    mean_age = df.Age.mean()
    return mean_age

get_data = BashOperator(
    task_id="get-data",
    bash_command='''curl https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv 
    -o /usr/local/airflow/data/titanic.csv'''
)

## Definição das tasks

task_calculate_mean = PythonOperator(
    task_id='calculate-mean-age',
    python_callable=calculate_mean_age,
    dag=dag
)

def print_age(**context):
    value=context['task_instance'].xcom_pull(task_ids='calculate-mean-age')
    print(f'A idade média é: {value}')

task_mean_age = PythonOperator(
    task_id="say-mean-age",
    python_callable=print_age,
    provide_context=True,
    dag=dag
)

task_print_age = PythonOperator(
    task_id="print-mean-age",
    python_callable=print_age,
    dag=dag
)

get_data >> task_mean_age >> task_print_age