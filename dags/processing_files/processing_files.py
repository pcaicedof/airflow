from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import dates
from airflow.models import TaskInstance, XCom, DagRun
from datetime import timedelta, datetime, timezone
from os import listdir
import pandas as pd

columns = {
    'jobs' : {'id': int, 'job': str},
    'departments' : {'id': int, 'department': str},
    'hired_employees' : {'id': "Int64", 'name': str, 'datetime': str, 'department_id': "Int64", 'job_id': "Int64"}
}


default_args = {
    'owner': 'pecafa',
    'depends_on_past': False,
    'email': ['pcaicedof@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

input_folder='/home/pedrodev/Documents/pcf_repository/airflow_dags/dags/processing_files/data'
input_extension = '.csv'


def get_file_list(folder, extension):
    try:
        file_list = [file for file in listdir(folder) if file.endswith(extension) is True]
        
    except FileNotFoundError:
            print(f'folder {self.folder} does not exist')
            file_list = []
    return file_list

def upload_files_to_raw(file_name, folder):
    file = f'{folder}/{file_name}'
    table = file_name.replace('.csv','')
    print(table)
    table_columns = list(columns[table].keys())
    print(table_columns)
    df_file = pd.read_csv(file, sep=',', names=table_columns, header=None)
    print(df_file.head())
    print(f"process file {file_name}")


dag = DAG(
    dag_id='processing_files',
    default_args=default_args,
    description='Processing files',
    schedule_interval='@daily',
    start_date=dates.days_ago(2),
    tags=['processing_files'],
    catchup=False,
)

start_task = DummyOperator(
    task_id='start_task',
    dag=dag,
)

# Tarea final
end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
)

file_list = get_file_list(input_folder, input_extension)

tasks=[]
for file_name in file_list:
    extract_files = PythonOperator(
        task_id= f"extract_data_from_{file_name.replace('.', '_')}",
        python_callable=upload_files_to_raw,
        op_args=[file_name, input_folder],
        provide_context=True,
        dag=dag
    )
    tasks.append(extract_files)


start_task >> tasks >> end_task


