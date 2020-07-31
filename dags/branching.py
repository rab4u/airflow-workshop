from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
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
    dag_id="branching-dag",
    default_args=default_args,
    catchup=False,
    schedule_interval="@once",
    is_paused_upon_creation=True)


def some_calculation(**kwargs):
    v1 = kwargs["params"]["var1"]
    v2 = kwargs["params"]["var2"]
    kwargs['ti'].xcom_push(key='result', value=v1 + v2)


def condition_select(**kwargs):
    result = kwargs['ti'].xcom_pull(key='result', task_ids='some_calculation_task')
    if result > 0:
        print("selecting task 1")
        return "task1"
    else:
        print("selecting task 2")
        return "task2"


some_calculation_task = PythonOperator(
    task_id='some_calculation_task',
    dag=dag,
    provide_context=True,
    python_callable=some_calculation,
    params={"var1": 10, "var2": 20},
)

print_result_task = BranchPythonOperator(
    task_id='print_result_task',
    dag=dag,
    provide_context=True,
    python_callable=condition_select,
)

task1 = BashOperator(
    task_id="task1",
    bash_command="echo Result is greater than 0",
    dag=dag,
)

task2 = BashOperator(
    task_id="task2",
    bash_command="echo Result is less than 0",
    dag=dag,
)

some_calculation_task >> print_result_task >> [task1, task2]
