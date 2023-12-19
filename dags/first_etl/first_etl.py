from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import dates
import pandas as pd
import os
from datetime import timedelta
from sqlalchemy import create_engine
import mysql.connector
#from utils.constants import data_types, connection

default_args = {
    'owner': 'pecafa',
    'depends_on_past': False,
    'email': ['pcaicedof@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

data_types = {
    'candidate' : {
        'First Name' : {'column_name' : 'first_name',
                   'data_type' : 'string'},
        'Last Name' : {'column_name' : 'last_name',
                    'data_type' : 'string'},
        'Email' : {'column_name' : 'email',
                    'data_type' : 'string'},
        'Application Date' : {'column_name' : 'application_date',
                    'data_type' : 'date'},
        'Country' : {'column_name' : 'country',
                    'data_type' : 'string'},
        'YOE' : {'column_name' : 'years_of_experience',
                    'data_type' : 'int'},
        'Seniority' : {'column_name' : 'seniority',
                    'data_type' : 'string'},
        'Technology' : {'column_name' : 'technology',
                    'data_type' : 'string'},
        'Code Challenge Score' : {'column_name' : 'code_challenge_score',
                    'data_type' : 'int'},
        'Technical Interview Score' : {'column_name' : 'technical_interview_score',
                    'data_type' : 'int'}
    }
}

connection = {
    'connector': 'mysql+mysqlconnector',
    'host': 'localhost',
    'database': 'fullstack_db',
    'user': 'pedro',
    'password': 'c41c3d0',
    'port': '3306'
}

def extract_data_to_raw(input_path, output_path):
    sep = ';'
    df = pd.read_csv(input_path, sep=sep)
    df.to_csv(output_path, encoding='utf-8', index=False)

def cast_fields(df, field, field_type):
    print(df.columns)
    if field_type == 'date':
        df[field] =  pd.to_datetime(df[field], format="%Y-%m-%d")
    elif field_type == 'string':
        df[field] = df[field].astype(str)
    elif field_type == 'int':
        df[field] = df[field].astype(int)
   
def clean_data(table_name, input_path, output_path):
    df_raw = pd.read_csv(input_path, sep=',')
    table_types = data_types[table_name]
    new_columns = {}
    new_columns = {key:value['column_name'] for key, value in table_types.items()}
    print(new_columns)
    df_rename = df_raw.rename(columns=new_columns)
    print(list(df_rename.columns))
    fields = table_types.keys()
    for field in fields:
        field_type = table_types[field]['data_type']
        new_field = table_types[field]['column_name']
        try:
            cast_fields(df_rename, new_field, field_type)
        except Exception as e:
            print(e)
        print(f'cast {field}')
    df_rename.to_csv(output_path, encoding='utf-8', index=False)
    print(df_rename.columns)

def load_data(table_name, input_path):
    df_clean = pd.read_csv(input_path, sep=',')
    connector = connection['connector']
    host=  connection['host']
    database=  connection['database']
    user=  connection['user']
    password=  connection['password']
    port=  connection['port']
    engine = create_engine(f'{connector}://{user}:{password}@{host}:{port}/{database}', echo=False)
    df_clean.to_sql(name=table_name, con=engine, if_exists = 'replace', index=False)

with DAG(
    'First_ETL',
    default_args=default_args,
    description='First ETL',
    schedule_interval='@daily',
    start_date=dates.days_ago(2),
    tags=['first_etl'],
) as dag:

    read_task = PythonOperator(
        task_id="extract_data_to_raw",
        python_callable=extract_data_to_raw,
        op_args=['./data/first_etl/candidate.csv','./data/first_etl/raw/raw_candidate.csv']
        )
    
    clean_data = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data,
        op_args=['candidate', './data/first_etl/raw/raw_candidate.csv','./data/first_etl/transformed/clean_candidate.csv']
        )

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        op_args=['candidate', './data/first_etl/transformed/clean_candidate.csv']
        )

    read_task >> clean_data >> load_data