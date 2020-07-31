from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Define the DAG
dag = DAG(
    dag_id="trigger_target_dag",
    default_args={"owner": "airflow", "start_date": days_ago(2)},
    schedule_interval=None,
)

def run_this_func(ds, **kwargs):
    print("Remotely received value of {} for key=message".
          format(kwargs['dag_run'].conf['message']))


py_task = PythonOperator(
    task_id='python_task',
    provide_context=True,
    python_callable=run_this_func,
    dag=dag,
)


bash_task = BashOperator(
    task_id="bash_task",
    bash_command='echo "Here is the message: '
                 '{{ dag_run.conf["message"] if dag_run else "" }}" ',
    dag=dag,
)

py_task >> bash_task
