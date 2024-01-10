import pandas as pd
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import dates
from airflow.models import TaskInstance, XCom, DagRun
from datetime import timedelta, datetime, timezone
from os import listdir


import sys
sys.path.insert(0, '/home/pedrodev/Documents/pcf_repository/airflow_dags/dags/processing_files/utils')

from constants import columns, input_folder, input_extension, \
    raw_folder, raw_extension, clean_folder, data_types

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s | %(name)s'
                           '| %(levelname)s | %(message)s')



default_args = {
    'owner': 'pecafa',
    'depends_on_past': False,
    'email': ['pcaicedof@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}


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
    table_columns = list(columns[table].keys())
    df_file = pd.read_csv(file, sep=',', names=table_columns, header=None)
    df_file.to_csv(f'{input_folder}/raw/{file_name}', index=False)
    logging.info(f"process file {file_name}")

def cast_fields(df, field, field_type):
    if field_type == 'date':
        df[field] =  pd.to_datetime(df[field], format="%Y-%m-%dT%H:%M:%SZ")
    elif field_type == 'string':
        df[field] = df[field].astype(str)
    elif field_type == 'int':
        df[field] = df[field].astype("Int64")
    logging.info(f"{field} has been sanitized")
   
def cleaning_data(table_name, input_path, output_path):
    df_raw = pd.read_csv(f'{input_path}/{table_name}', sep=',')
    table_types = data_types[table_name.replace('.csv','')]
    new_columns = {}
    new_columns = {key:value['column_name'] for key, value in table_types.items()}
    df_rename = df_raw.rename(columns=new_columns)
    fields = table_types.keys()
    for field in fields:     
        field_type = table_types[field]['data_type']
        new_field = table_types[field]['column_name']
        logging.info(f"casting {field} to {field_type}")
        try:
            cast_fields(df_rename, new_field, field_type)
        except Exception as e:
            logging.error(f"field {field} has not been casted {e}")
    df_rename.to_csv(f'{output_path}/{table_name}', encoding='utf-8', index=False)
    print(df_rename.columns)


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

middle_task = DummyOperator(
    task_id='middle_task',
    dag=dag,
)

# Tarea final
end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
)

file_list = get_file_list(input_folder, input_extension)

extract_task=[]
for file_name in file_list:
    extract_files = PythonOperator(
        task_id= f"extract_data_from_{file_name.replace('.', '_')}",
        python_callable=upload_files_to_raw,
        op_args=[file_name, input_folder],
        provide_context=True,
        dag=dag
    )
    extract_task.append(extract_files)

raw_list = get_file_list(raw_folder, raw_extension)
 
clean_task=[]
for file_name in file_list:
    clean_data = PythonOperator(
        task_id= f"clean_data_from_{file_name.replace('.', '_')}",
        python_callable=cleaning_data,
        op_args=[file_name, raw_folder, clean_folder],
        provide_context=True,
        dag=dag
    )
    clean_task.append(clean_data)


start_task >> extract_task >> middle_task >> clean_task >> end_task



