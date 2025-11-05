from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.operators.python import PythonOperator
from airflow.utils.timezone import convert_to_utc
import pandas as pd

# Параметры DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_postgres_to_clickhouse',
    default_args=default_args,
    description='ETL',
    schedule='@hourly',
    catchup=False,
)


def extract_from_telemetry_pg(**context):
    execution_date = context['execution_date']
    end_time = convert_to_utc(execution_date)
    start_time = end_time - timedelta(hours=1)
    sql = f"""
        select user_id,
               telemetry,
               now() as date
        from telemetry
        where created_at between '{start_time}' and '{end_time}'
    """
    hook = PostgresHook(postgres_conn_id='telemetry_db')
    conn = hook.get_conn()
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


def extract_from_users_pg(**context):
    sql = f"""
        select id as user_id, 
               name
        from users
    """
    hook = PostgresHook(postgres_conn_id='users_db')
    conn = hook.get_conn()
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


def merge_data(**context):
    df1 = context['task_instance'].xcom_pull(task_ids='extract_from_telemetry_pg')
    df2 = context['task_instance'].xcom_pull(task_ids='extract_from_users_pg')
    merged_df = pd.merge(df1, df2, on='user_id', how='inner')
    return merged_df


def load_to_clickhouse(**context):
    df = context['task_instance'].xcom_pull(task_ids='merge_data')
    hook = ClickHouseHook(clickhouse_conn_id='clickhouse_db')
    hook.insert_dataframe(df, 'combined_data')


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
