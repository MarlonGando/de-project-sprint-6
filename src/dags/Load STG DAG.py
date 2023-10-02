import boto3
import vertica_python
from datetime import datetime
from airflow.decorators import dag
from airflow import DAG
from airflow.operators.python import PythonOperator



AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"


conn_info = {'host': 'vertica.tgcloudenv.ru', 
             'port': '5433',
             'user': 'stv2023081266',       
             'password': 'Ug5ViT1V6kBkwNU',
             'database': 'dwh',
             'autocommit': True
}


def fetch_s3_file(bucket: str, key: str) -> str:    
    session = boto3.session.Session()
    s3_client = session.client(
        service_name = 's3',
        endpoint_url = 'https://storage.yandexcloud.net',
        aws_access_key_id = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=f'/data/{key}'
    )


def load_to_stg(file, schema, table, columns):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(f"""COPY {schema}.{table} {columns}
                        FROM LOCAL '/data/{file}'
                        DELIMITER ','
                        REJECTED DATA AS TABLE {schema}.{table}_rej
                        ;""")


files = ('users.csv','groups.csv','dialogs.csv', 'group_log.csv')

dag = DAG(
    schedule_interval=None,
    dag_id='loading_into_stg',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['STAGING'],
    is_paused_upon_creation=True)

fetch_tasks = [PythonOperator(task_id=f'fetch_{key}',
                                python_callable=fetch_s3_file,
                                op_kwargs = {'bucket': 'sprint6', 'key': key},)
                                for key in files
    ]

loading_into_users_stg = PythonOperator(task_id='loading_into_user',
                                 python_callable=load_to_stg,
                                 op_kwargs={'file': 'users.csv', 'schema': 'STV2023081266__STAGING', 'table':'users', 'columns':'( id, chat_name, registration_dt, country, age)'},
                                 dag=dag)
loading_into_groups_stg = PythonOperator(task_id='loading_into_groups',
                                 python_callable=load_to_stg,
                                 op_kwargs={'file': 'groups.csv', 'schema': 'STV2023081266__STAGING', 'table':'groups', 'columns':'( id, admin_id, group_name, registration_dt, is_private)'},
                                 dag=dag)
loading_into_dialogs_stg = PythonOperator(task_id='loading_into_dialogs',
                                 python_callable=load_to_stg,
                                 op_kwargs={'file': 'dialogs.csv', 'schema': 'STV2023081266__STAGING', 'table':'dialogs', 'columns':'( message_id,message_ts,message_from,message_to,message,message_group)'},
                                 dag=dag)
loading_into_group_log_stg = PythonOperator(task_id='loading_into_group_log',
                                 python_callable=load_to_stg,
                                 op_kwargs={'file': 'group_log.csv', 'schema': 'STV2023081266__STAGING', 'table':'group_log', 'columns':'( group_id, user_id, user_id_from, event, event_dt)'},
                                 dag=dag)


fetch_tasks  >> loading_into_users_stg >> loading_into_groups_stg >> loading_into_dialogs_stg >> loading_into_group_log_stg





