from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pandas as pd


default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

dag = DAG(
    'test_pg',
    default_args=default_args,
    description='ETL',
    schedule='@hourly',
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
)


def test_pg_connection1(**context):
    sql = f"""
        select id as user_id, 
               name
        from users
    """
    with PostgresHook(postgres_conn_id='users_db1').get_conn() as conn:
        with conn.cursor() as cursor:
            res = cursor.execute(sql)
    return res
    # context['ti'].xcom_push(key='users1', values=res)


def test_pg_connection2(**context):
    sql = f"""
        select id as user_id, 
               name
        from users
    """
    with PostgresHook(postgres_conn_id='users_db2').get_conn() as conn:
        with conn.cursor() as cursor:
            res = cursor.execute(sql)
    return res
    # context['ti'].xcom_push(key='users2', values=res)


extract1 = PythonOperator(
    task_id='test_default_port',
    python_callable=test_pg_connection1,
    dag=dag,
)


extract2 = PythonOperator(
    task_id='test_custom_port',
    python_callable=test_pg_connection2,
    dag=dag,
)

extract1
extract2