import os
import pathlib
import boto3
import vertica_python
from datetime import datetime
from airflow.decorators import dag
from airflow import DAG
from airflow.operators.python import PythonOperator



#AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
#AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"


conn_info = {'host': 'vertica.tgcloudenv.ru', 
             'port': '5433',
             'user': 'stv2023081266',       
             'password': 'Ug5ViT1V6kBkwNU',
             'database': 'dwh',
             'autocommit': True
}

'''f'src/sql/DDL DWH objects.sql'
f'src/sql/DML DWH data.sql'

def ddl(table_schema: str, suffix: str):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        script_name = f'src/sql/DDL DWH objects.sql'
        cur.execute(open(script_name, 'r').read())
    log.info(f'Проверка {table_schema}{suffix} завершена успешно')

def load_to_dwh(table_schema: str, suffix: str):
    """Функция принимает наименование схемы и суффикс типа таблицы
        и загружает данные в таблицу"""
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        script_name = f'src/sql/loaddata_{table_schema}{suffix}.sql'
        cur.execute(open(script_name, 'r').read())
    log.info(f'Данные загружены в таблицы {table_schema}{suffix}')'''


def fetch_sql_query_from_a_file(path: str):
    with open(path) as file:
        return file.read()


def run_sql_query(query: str):
    with vertica_python.connect(**conn_info) as con:
        with con.cursor() as cur:
            cur.execute(query)
        con.commit()



def worker(file):
    directory = os.path.join(pathlib.Path(__file__).parent.absolute(), 'sql')
    path = f'{directory}/{file}'
    
    #path = f'C:/Users/Grek/Desktop/Practicum/Sprint 6/de-project-sprint-6/src/sql/{file}'
    #path = f'src/sql/{file}'
    query = fetch_sql_query_from_a_file(path)
    run_sql_query(query)


dag = DAG(
    schedule_interval=None,
    dag_id='active_actions',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['DDS'],
    is_paused_upon_creation=True)

DDL = PythonOperator(task_id='DDL',
                     python_callable=worker,
                     op_kwargs={'file': 'DDL_DWH_objects.sql'},
                     dag=dag)

DML = PythonOperator(task_id='DML',
                     python_callable=worker,
                     op_kwargs={'file': 'DML_DWH_data.sql'},
                     #op_kwargs={'file': 'src/sql/DML DWH data.sql'},
                     dag=dag)

DDL >> DML