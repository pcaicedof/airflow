
connection = {
    'connector': 'mysql+mysqlconnector',
    'host': 'localhost',
    'database': 'fullstack_db',
    'user': 'pedro',
    'password': 'c41c3d0',
    'port': '3306'
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