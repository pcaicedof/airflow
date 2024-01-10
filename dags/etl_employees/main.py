#airflow
from airflow.decorators import dag, task
from airflow.models.dag import DAG
from airflow import settings
from airflow.models import Connection
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
#python
from datetime import timedelta, datetime
from os import listdir
import logging
import pandas as pd
import sys
import psycopg2
from sqlalchemy import create_engine, text
from decouple import config

sys.path.insert(0, '/home/pedrodev/Documents/pcf_repository/airflow_dags/dags/etl_employees/utils')

from constants import columns, INPUT_FOLDER, INPUT_EXTENSION, \
    RAW_FOLDER, RAW_EXTENSION, CLEAN_FOLDER, data_types, dbt_profile_dir, \
    dbt_project_dir, run_command, test_command

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s | %(name)s'
                           '| %(levelname)s | %(message)s')

DB_PARAMS = {
    'host': config('DB_HOST'),
    'database': config('DB_NAME'),
    'user': config('DB_USER'),
    'password': config('DB_PASSWORD')
}



default_args = {
    'owner': 'pecafa',
    'depends_on_past': False,
    'email': ['pcaicedof@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

email_subject = "prueba envìo airflow"
email_to = ["pcaicedof@gmail.com"]
email_html_content = "<p>Hola, esta es una prueba.</p>"


@dag(
    dag_id='dynamic_tasks',
    start_date=datetime(2024, 1, 2),
    schedule_interval='@daily',
    default_args=default_args
)
def etl():

    @task
    def get_file_list(folder, extension):
        try:
            file_list = [file for file in listdir(folder) if file.endswith(extension) is True]
            logging.info(f"files found are {file_list}")
        except FileNotFoundError:
                logging.error(f'folder {folder} does not exist')
                file_list = []
        return file_list

    @task
    def upload_file_to_raw(file_name, input_folder, raw_folder):
        file = f'{input_folder}/{file_name}'
        table = file_name.replace('.csv','')
        table_columns = list(columns[table].keys())
        df_file = pd.read_csv(file, sep=',', names=table_columns, header=None)
        try:
            df_file.to_csv(f'{raw_folder}/{file_name}', index=False)
            logging.info(f"file load to {raw_folder}/{file_name} ")
            processed_file = f'{raw_folder}/{file_name}'
        except Exception as e:
            logging.error(f"{file_name} has not been loaded {e}")
            processed_file = ''
        finally:
            return processed_file


    @task
    def get_task_list(task_sent):
        task_list = list(task_sent)
        for tarea in task_list:
            print(tarea)
        return task_list
    
    def cast_fields(df, field, field_type):
        if field_type == 'date':
            df[field] =  pd.to_datetime(df[field], format="%Y-%m-%dT%H:%M:%SZ")
        elif field_type == 'string':
            df[field] = df[field].astype(str)
        elif field_type == 'int':
            df[field] = df[field].astype("Int64")
        logging.info(f"{field} has been sanitized")

    @task
    def cleaning_data(file, output_path):
        logging.info(f"{file} is being cleaned")
        df_raw = pd.read_csv(f'{file}', sep=',')
        table_name = file.split('/')[-1].replace('.csv','')
        cleaned_file = f'{output_path}/{table_name}.csv'
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
        df_rename.to_csv(cleaned_file, encoding='utf-8', index=False)
        print(df_rename.columns)
        return cleaned_file

    @task
    def load_csv_to_postgres(file):
        table_name = file.split('/')[-1].replace('.csv','')
        engine = create_engine(f'postgresql://{DB_PARAMS["user"]}:{DB_PARAMS["password"]}@{DB_PARAMS["host"]}/{DB_PARAMS["database"]}')
        df = pd.read_csv(file)
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        return 'table loaded'

    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command = run_command,
        do_xcom_push=True
    )

    run_dbt_test = BashOperator(
        task_id='run_dbt_test',
        bash_command = f'{test_command} 2>&1',
        do_xcom_push=True
    )

    def get_number_of_errors(texto):
        inicio = texto.find('ERROR')
        first_string = texto[inicio:]
        fin= first_string.find(' ')
        value = int(first_string[:fin].replace("ERROR=",""))
        return value

    def get_test_error(**kwargs):
        ti = kwargs['ti']
        test_result = ti.xcom_pull(task_ids='run_dbt_test', key='return_value')
        quantity_error = get_number_of_errors(test_result)
        if quantity_error > 0:
            return 'send_email'
        else:
            return 'end_task'
    
    get_test_result = BranchPythonOperator(
        task_id='get_test_result',
        python_callable=get_test_error,
        provide_context=True
    )
    
    send_email = EmailOperator(
        task_id='send_email',
        to=email_to,
        subject=email_subject,
        html_content=email_html_content
        )
    

    #send_email = DummyOperator(task_id='send_email')
    end_task = DummyOperator(task_id='end_task')
    
    file_list = get_file_list(folder=INPUT_FOLDER,extension=INPUT_EXTENSION)
    processed_files = upload_file_to_raw.partial(input_folder=INPUT_FOLDER,raw_folder=RAW_FOLDER).expand(file_name=file_list)
    # La variable tasks_expanded se utiliza como entrada para la función get_task_list
    cleaning_files = cleaning_data.partial(output_path=CLEAN_FOLDER).expand(file=processed_files)
    result = load_csv_to_postgres.expand(file=cleaning_files)
    
    result >> run_dbt_models >>run_dbt_test >> get_test_result >> [send_email, end_task]
    send_email >> end_task


if __name__ == "__main__":
    etl_instance = etl()