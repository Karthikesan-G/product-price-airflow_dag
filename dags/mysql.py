from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.books_to_scrape import main as scrape_books
from scripts.insert_data import insert_data as insert_books_data, check_price_diff
from airflow.hooks.base import BaseHook

default_args = {
    'owner': 'Karthik',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='mysql_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    scrape_books_task = PythonOperator(
        task_id = 'scrape_books',
        python_callable = scrape_books
    )

    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id='mysql_conn',
        sql="""
        create table if not exists books (
            id int auto_increment primary key,
            Date_Added datetime not null,
            Title varchar(1000) not null,
            Descriptions text,
            Category_Name varchar(1000),
            Star_Rating decimal(10,2),
            Price decimal(10,2) not null,
            Tax decimal(10,2),
            Price_with_Tax decimal(10,2) not null,
            Availability varchar(100),
            Availability_Count int,
            UPC  varchar(100) not null,
            Product_Type  varchar(100),
            Number_of_Reviews int,
            Image_URL  varchar(1000),
            Book_Link varchar(1000)
        )
        """
    )

    conn = BaseHook.get_connection("mysql_conn")

    insert_data = PythonOperator(
        task_id='insert_data',
        python_callable = insert_books_data,
        op_kwargs={
            'host': conn.host,
            'user': conn.login,
            'password': conn.password,
            'database': conn.schema
        }
    )

    # check_price_diff = PythonOperator(
    #     task_id = 'price_diff',
    #     mysql_conn_id='mysql_conn',
    #     sql="""
    #         select 
    #             b1.id,
    #             b1.Title,
    #             (b2.Price - b1.Price) as Price_Diff
    #         from books b1
    #         join books b2
    #         on b1.Date_Added = DATE_sub(b2.Date_Added, interval 1 day) 
    #         and b1.UPC = b2.UPC
    #         where (b2.Price - b1.Price) > 10
    #     """
    # )

    check_price_diff_t = PythonOperator(
        task_id = 'check_price_diff',
        python_callable = check_price_diff,
        op_kwargs = {
            'host': conn.host,
            'user': conn.login,
            'password': conn.password,
            'database': conn.schema
        }
    )

    

    # scrape_books_task >> create_table >> check_price_diff_t
    create_table >> check_price_diff_t
