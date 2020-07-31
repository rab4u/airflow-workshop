from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago


def conditionally_trigger(context, dag_run_obj):
    c_p = context['params']['condition_param']
    print("Controller DAG : conditionally_trigger = {}".format(c_p))
    if context['params']['condition_param']:
        dag_run_obj.payload = {'message': context['params']['message']}
        return dag_run_obj


# Define the DAG
dag = DAG(
    dag_id="trigger_controller_dag",
    default_args={"owner": "airflow", "start_date": days_ago(2)},
    schedule_interval="@once",
)

trigger = TriggerDagRunOperator(
    task_id='trigger_controller_task',
    trigger_dag_id="trigger_target_dag",
    python_callable=conditionally_trigger,
    params={'condition_param': True, 'message': 'Hello World'},
    dag=dag,
)