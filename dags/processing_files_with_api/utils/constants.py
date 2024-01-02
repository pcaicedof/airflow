columns = {
    'jobs' : {'id': int, 'job': str},
    'departments' : {'id': int, 'department': str},
    'hired_employees' : {'id': "Int64", 'name': str, 'datetime': str, 'department_id': "Int64", 'job_id': "Int64"}
}

data_types = {
    'departments' : {
        'id' : {'column_name' : 'department_id',
                   'data_type' : 'int'},
        'department' : {'column_name' : 'department_name',
                    'data_type' : 'string'}
    },
    'jobs': {
        'id' : {'column_name' : 'job_id',
                   'data_type' : 'int'},
        'job' : {'column_name' : 'job_name',
                    'data_type' : 'string'}
    },
    'hired_employees': {
        'id' : {'column_name' : 'job_id',
                   'data_type' : 'int'},
        'name' : {'column_name' : 'employee_name',
                    'data_type' : 'string'},
        'datetime' : {'column_name' : 'hire_date',
                    'data_type' : 'date'},
        'department_id' : {'column_name' : 'department_id',
                    'data_type' : 'int'},
        'job_id' : {'column_name' : 'job_id',
                    'data_type' : 'int'}
    }
}

input_folder='/home/pedrodev/Documents/pcf_repository/airflow_dags/dags/processing_files_with_api/data'
input_extension = '.csv'

raw_folder='/home/pedrodev/Documents/pcf_repository/airflow_dags/dags/processing_files_with_api/data/raw'
raw_extension = '.csv'

clean_folder='/home/pedrodev/Documents/pcf_repository/airflow_dags/dags/processing_files_with_api/data/transformed'
