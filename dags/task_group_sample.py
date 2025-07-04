from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup

def push_func(ti):
    ti.xcom_push(key='msg', value='Hi da thailee')
    
def pull_func(ti):
    msg = ti.xcom_pull(key='msg')
    print(msg)



default_args = {
    'owner' : 'karthik',
    'start_date' : datetime(2025, 7, 2),
    'retries' : 2,
    'retry_delay': timedelta(minutes=1),
    'email': '',
    'email_on_failure': False,
    'email_on_sucess': False
}


with DAG(
    dag_id = 'task_group_ex_dag',
    description = 'a small sample dag to practice the task group function and other functions',
    default_args = default_args,
    schedule_interval = '@daily'
    ) as dag:
    
    t1 = PythonOperator(
        task_id = 'start_t1',
        python_callable = push_func
    )
    
    t2 = PythonOperator(
        task_id = 'start_t2',
        python_callable = pull_func
    )
    
    te = PythonOperator(
        task_id = 'end_t',
        python_callable = lambda: print("Done")
    )
    
    with TaskGroup(
        group_id = 'group_1'
    ) as tg1:
        
        tg_t1 = DummyOperator(task_id = 'tg_t1')
        tg_t2 = DummyOperator(task_id = 'tg_t2')
        tg_t3 = DummyOperator(task_id = 'tg_t3')
        tg_t4 = DummyOperator(task_id = 'tg_t4')
        
        
        [tg_t1, tg_t2] >> tg_t3 >> tg_t4
    
    [t1, t2] >> tg1 >> te