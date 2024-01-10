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
        'id' : {'column_name' : 'id',
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

INPUT_FOLDER='/home/pedrodev/Documents/pcf_repository/airflow_dags/dags/etl_employees/data/input'
INPUT_EXTENSION = '.csv'

RAW_FOLDER='/home/pedrodev/Documents/pcf_repository/airflow_dags/dags/etl_employees/data/raw'
RAW_EXTENSION = '.csv'

CLEAN_FOLDER='/home/pedrodev/Documents/pcf_repository/airflow_dags/dags/etl_employees/data/transformed'

dbt_profile_dir = '/home/pedrodev/.dbt'
dbt_project_dir = '/home/pedrodev/Documents/pcf_repository/dbt/dbt_test'

run_command = f"dbt run \
    --profiles-dir {dbt_profile_dir} \
    --project-dir {dbt_project_dir}"

test_command = f"dbt test \
    --profiles-dir {dbt_profile_dir} \
    --project-dir {dbt_project_dir} || true"