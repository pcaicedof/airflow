import pandas as pd
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import dates
from airflow.models import TaskInstance, XCom, DagRun
from airflow.decorators import dag, task

from datetime import timedelta, datetime, timezone
from os import listdir


import sys
sys.path.insert(0, '/home/pedrodev/Documents/pcf_repository/airflow_dags/dags/processing_files_with_api/utils')

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

@dag(
    dag_id='etl_with_api',
    default_args=default_args,
    start_date=datetime(2023, 12, 26),
    schedule_interval='@daily'
    )
def etl_csv_to_final():

    @task()
    def get_file_list(folder, extension):
        try:
            file_list = [file for file in listdir(folder) if file.endswith(extension) is True]
            logging.info(f"files found are {file_list}")
        except FileNotFoundError:
                logging.error(f'folder {self.folder} does not exist')
                file_list = []
        return file_list
    

    def upload_file_to_raw(file_name, folder):
        file = f'{folder}/{file_name}'
        table = file_name.replace('.csv','')
        table_columns = list(columns[table].keys())
        df_file = pd.read_csv(file, sep=',', names=table_columns, header=None)
        print(df_file.head())
        df_file.to_csv(f'{input_folder}/raw/{file_name}', index=False)
        logging.info(f"file load to {input_folder}/raw/{file_name} ")
        return f"process file {file_name}"

    @task()
    def load_files_from_list(file_list):
        raw_list = []
        for file in file_list:
            file_process = upload_file_to_raw(file, input_folder)
            logging.info(file_process)
            raw_list.append(file)
        return raw_list

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
    
    @task()
    def cleaning_data_from_list(file_list):
        clean_list = []
        for file in file_list:
            file_process = cleaning_data(file, raw_folder, clean_folder)
            logging.info(file_process)
            clean_list.append(file)
        return clean_list


    file_list = get_file_list(input_folder, input_extension)
    raw_list = load_files_from_list(file_list)
    clean_list =  cleaning_data_from_list(raw_list)

    #for file in file_list:
    #    upload_files_to_raw(file, input_folder)


etl = etl_csv_to_final()