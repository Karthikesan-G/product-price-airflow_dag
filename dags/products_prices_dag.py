from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from datetime import datetime, timedelta
import pandas as pd
import os

# scripts
from scripts.fakestoreapi.get_products import fetch_products, clean_products, load_data


def format_email_and_save_csv(ti):
    results = ti.xcom_pull(task_ids='price_compare')
    if not results:
        return "<p>No price drops</p>"

    df = pd.DataFrame(results, columns=['product_name', 'yesterday_price', 'today_price', 'drop_percentage'])
    report_dir = ti.xcom_pull(key='path')
    csv_path = os.path.join(report_dir, 'price_drop.csv')
    ti.xcom_push(key='csv_path', value=csv_path) 
    df.to_csv(csv_path, index=False)
    return "<p>price drop report attached</p>"


# def success_slack_msg(context):
#     SlackWebhookOperator(
#         task_id='slack_notify_success',
#         message="Product Price DAG completed successfully!",
#         slack_webhook_conn_id="slack_conn"
#     ).execute(context=context)

default_args = {
    'owner': 'karthik',
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    # 'on_success_callback': success_slack_msg,
    'email_on_failure': False
}

with DAG(
    dag_id='products_prices_dag',
    default_args=default_args,
    description='Track product prices and alert on drops',
    schedule_interval='@daily',
    catchup=False
) as dag:

    start = DummyOperator(task_id='start_dag')

    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id='mysql_conn',
        sql="""
        CREATE TABLE IF NOT EXISTS products_table (
            product_id INT NOT NULL,
            product_name VARCHAR(200),
            price DECIMAL(10,2),
            category VARCHAR(100),
            description TEXT,
            image_link VARCHAR(200),
            rating DECIMAL(10,2),
            rating_count INT,
            date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(product_id, date)
        );
        """
    )

    file_path = PythonOperator(
        task_id = 'file_path',
        python_callable = lambda ti: ti.xcom_push(key='path', value='/opt/airflow/dags/scripts/fakestoreapi/')
    )

    with TaskGroup(group_id='etl_group') as etl:

        fetch = PythonOperator(
            task_id='fetch_products',
            python_callable=fetch_products
        )

        transform = PythonOperator(
            task_id='transform_products',
            python_callable=clean_products
        )

        conn = BaseHook.get_connection('mysql_conn')
        load = PythonOperator(
            task_id='load_products',
            python_callable=load_data,
            op_kwargs={
                'host': conn.host,
                'user': conn.login,
                'password': conn.password,
                'database': conn.schema
            }
        )

        fetch >> transform >> load

    price_compare = MySqlOperator(
        task_id='price_compare',
        mysql_conn_id='mysql_conn',
        do_xcom_push=True,
        sql="""
            SELECT 
                pt1.product_name, 
                pt1.price AS yesterday_price, 
                pt2.price AS today_price, 
                ROUND(((pt1.price - pt2.price) / pt1.price) * 100, 2) AS drop_percentage
            FROM products_table pt1
            JOIN products_table pt2
              ON pt1.product_id = pt2.product_id
             AND DATE(pt1.date) = DATE_SUB(DATE(pt2.date), INTERVAL 1 DAY)
            WHERE pt1.price > pt2.price;
        """
    )

    with TaskGroup(group_id='mail_group') as mail:

        prepare_email = PythonOperator(
            task_id='generate_email',
            python_callable=format_email_and_save_csv
        )

        send_email = EmailOperator(
            task_id='send_alert',
            to='karthikesan.in@gmail.com',
            subject='Daily Price Drop Alert',
            html_content="{{ ti.xcom_pull(task_ids='mail_group.generate_email') }}",
            files=["{{ ti.xcom_pull(task_ids='mail_group.generate_email', key='csv_path') }}"],
            conn_id='email_conn'
        )

        prepare_email >> send_email

    end = DummyOperator(task_id='end_dag')

    start >> [create_table, file_path] >> etl >> price_compare >> mail >> end
