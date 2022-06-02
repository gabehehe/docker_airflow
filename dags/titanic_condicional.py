from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random

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
    description="Carrega dados condicionalmente para homens ou mulheres a bordo do Titanic",
    default_args=default_args,
    schedule_interval=timedelta(minutes=3)
)

## Definição de funções para os operators Python

def calculate_mean_age():
    df = pd.read_csv('/usr/local/airflow/data/titanic.csv')
    mean_age = df.Age.mean()
    return mean_age

def print_age(**context):
    value=context['task_instance'].xcom_pull(task_ids='calculate-mean-age')
    print(f'A idade média é: {value}')

def m_or_f(**context):
    value = context['task_instance'].xcom_pull(task_ids='m_or_f'),
    if value == 'male':
        return 'branch_homem'
    if value == 'female':
        return 'branch_mulher'


## Definição das tasks

get_data = BashOperator(
    task_id="get-data",
    bash_command='''curl https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv 
    -o /usr/local/airflow/data/titanic.csv'''
)

male_female  = BranchPythonOperator(
    task_id="condicional",
    python_callable=lambda: 'm_or_f',
    provide_context=True,
    dag=dag
)




def mean_homem():
    df = pd.read_csv('/usr/local/airflow/data/train.csv')
    df = df.loc[df.Sex == 'male']
    print(f"Média de idade de homens no Titanic: {df.Age.mean()} ")

mean_homem = PythonOperator(
    task_id='branch_homem',
    python_callable=mean_homem,
    dag=dag
)

def mean_mulher():
    df = pd.read_csv('/usr/local/airflow/data/train.csv')
    df = df.loc[df.Sex == 'female']
    print(f"Média de idade de mulheres no Titanic: {df.Age.mean()} ")

mean_mulher = PythonOperator(
    task_id='branch_mulher',
    python_callable=mean_mulher,
    dag=dag
)

def pick_h_m():
    return random.choice(['male', 'female'])

pick_h_m =  PythonOperator(
    task_id="pick_h_m",
    python_callable='pick_h_m',
    dag=dag
)

task_calculate_mean = PythonOperator(
    task_id='calculate-mean-age',
    python_callable=calculate_mean_age,
    dag=dag
)

task_mean_age = PythonOperator(
    task_id="say-mean-age",
    python_callable=mean_age,
    provide_context=True,
    dag=dag
)

task_print_age = PythonOperator(
    task_id="print-mean-age",
    python_callable=print_age,
    dag=dag
)

get_data >> pick_h_m >> male_female >> [branch_homem, branch_mulher]