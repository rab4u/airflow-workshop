from airflow import DAG
from airflow.operators.bash_operator import BashOperator
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
    dag_id="my-first-dag",
    default_args=default_args,
    catchup=False,
    schedule_interval="@once",
    is_paused_upon_creation=True)

t1 = BashOperator(
    task_id="HelloWorld",
    bash_command="echo 'Hello world'",
    dag=dag
    )

t2 = BashOperator(
    task_id="MyFirstDag",
    bash_command="echo 'Hurray, I created my first dag :)'",
    dag=dag
)

t1.set_upstream(t2)
