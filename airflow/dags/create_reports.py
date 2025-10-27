from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.clickhouse.operators.clickhouse import ClickHouseOperator
from datetime import datetime
import csv

# Аргументы по умолчанию: владелец процесса и время отсчёта для задачи
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 1),
}


# Функция для чтения данных и генерации SQL-запросов
def generate_insert_queries():
    CSV_FILE_PATH = 'sample_files/sample.csv'
    with open(CSV_FILE_PATH, 'r') as csvfile:
        csvreader = csv.reader(csvfile)

        # Генерим запросы
        insert_queries = []
        is_header = True
        for row in csvreader:
            if is_header:
                is_header = False
                continue
            insert_query = f"INSERT INTO sample_table (id,order_number,total,discount,buyer_id) VALUES ({row[0]}, {row[1]}, {row[2]},{row[3]},{row[4]});"
            insert_queries.append(insert_query)

        # Сохраняем запросы
        with open('./dags/sql/insert_queries.sql', 'w') as f:
            for query in insert_queries:
                f.write(f"{query}\n")


# Определяем DAG
with DAG(dag_id="reports",
         default_args=default_args,  # аргументы по умолчанию в начале скрипта
         schedule_interval="@hourly",  # запускаем один раз
         catchup=False) as dag:  # предотвращает повторное выполнение DAG для пропущенных расписаний.

    # get telemetry
    get_telemetry = PostgresOperator(
        task_id="get_telemetry",  # идентификатор задачи
        postgres_conn_id="telemetry_db",  # Название подключения
        sql="""select * from telemetry_table;"""
    )
    # get user data from crm
    get_users = PostgresOperator(
        task_id="get_users",
        postgres_conn_id="users_db",
        sql="""select * from users;"""
    )
    # combine data
    # insert into clickhouse
    insert_clickhouse = ClickHouseOperator(
        task_id="insert_clickhouse",
        clickhouse_conn_id="clickhouse_db",
        sql="""""",
    )



    # get_telemetry >> get_users >> combine_data >> insert_clickhouse





    # Опеределяем оператор для вставки данных
    # generate_queries = PythonOperator(
    #     task_id='generate_insert_queries',
    #     python_callable=generate_insert_queries
    # )
    #
    # # Запускаем выполнение оператора PostgresOperator
    # run_insert_queries = PostgresOperator(
    #     task_id='run_insert_queries',
    #     postgres_conn_id='write_to_postgres',  # Название подключения к PostgreSQL в Airflow UI
    #     sql='sql/insert_queries.sql'
    # )
    # create_table >> generate_queries >> run_insert_queries
    # Тут дальше можно продолжать пайплайн