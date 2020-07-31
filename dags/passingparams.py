from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "rab4u",
    "depends_on_past": False,
    "start_date": datetime(2020, 7, 25),
    "email": ["myemail@mail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="passing-params-dag",
    default_args=default_args,
    catchup=False,
    schedule_interval="@once",
    is_paused_upon_creation=True)


def print_params(**kwargs):
    print(" 'I passed a param = {}".format(kwargs["params"]["key"]))
    return 1


d1 = DummyOperator(
    task_id='dummy',
    dag=dag,
)

t1 = PythonOperator(
    task_id='task1',
    provide_context=True,
    python_callable=print_params,
    params={"key": "TASK 1 PARAM PASSED"},
    dag=dag,
)


t2 = PythonOperator(
    task_id='task2',
    provide_context=True,
    python_callable=print_params,
    params={"key": "TASK 2 PARAM PASSED"},
    dag=dag,
)

d1 >> [t1, t2]
