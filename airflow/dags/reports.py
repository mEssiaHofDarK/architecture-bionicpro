from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import clickhouse_connect
import pandas as pd

# Параметры DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

dag = DAG(
    'etl_postgres_to_clickhouse',
    default_args=default_args,
    description='ETL',
    schedule='@hourly',
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
)


def extract_from_telemetry_pg(**context):
    sql = f"""
        select user_id,
               telemetry,
               date
        from telemetry
    """
    with PostgresHook(postgres_conn_id='telemetry_db').get_conn() as conn:
        df = pd.read_sql(sql, conn)
    return df


def extract_from_users_pg(**context):
    sql = f"""
        select user_id, 
               name
        from users
    """
    with PostgresHook(postgres_conn_id='users_db').get_conn() as conn:
        df = pd.read_sql(sql, conn)
    return df


def merge_data(**context):
    df1 = context['task_instance'].xcom_pull(task_ids='extract_from_telemetry_pg')
    df2 = context['task_instance'].xcom_pull(task_ids='extract_from_users_pg')
    merged_df = pd.merge(df1, df2, on='user_id', how='inner')
    return merged_df


def load_to_clickhouse(**context):
    df = context['task_instance'].xcom_pull(task_ids='merge_data')
    prepared_data = [list(row.values()) for row in df.to_dict('records')]
    for item in prepared_data:
        print(item)
    client = clickhouse_connect.get_client(
        host="clickhouse",
        username="admin",
        password="turboadmin",
    )
    client.insert(table='reports', data=prepared_data, column_names=['user_id', 'telemetry', 'date', 'name'])


extract_1 = PythonOperator(
    task_id='extract_from_telemetry_pg',
    python_callable=extract_from_telemetry_pg,
    dag=dag,
)


extract_2 = PythonOperator(
    task_id='extract_from_users_pg',
    python_callable=extract_from_users_pg,
    dag=dag,
)


merge = PythonOperator(
    task_id='merge_data',
    python_callable=merge_data,
    dag=dag,
)


load = PythonOperator(
    task_id='load_to_clickhouse',
    python_callable=load_to_clickhouse,
    dag=dag,
)


extract_1 >> merge
extract_2 >> merge
merge >> load
