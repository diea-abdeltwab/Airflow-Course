# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime


def print_jinja_report( dag_id, task_id, run_date, run_id):
    print("====== Pipeline Report ======")
    print(f" DAG ID   : {dag_id}")
    print(f" Task ID  : {task_id}")
    print(f" Run Date : {run_date}")
    print(f" Run ID   : {run_id}")
    print("  Status   : Success")
    print("=============================")

# ===================== Report Function =====================
def print_context_report(**context):
    print("====== Pipeline Report ======")
    print(f" DAG ID   : {context['dag'].dag_id}")
    print(f" Task ID  : {context['task_instance'].task_id}")
    print(f" Run Date : {context['ds']}")
    print(f" Run ID   : {context['run_id']}")
    print("  Status   : Success")
    print("=============================")

# ===================== DAG Definition =====================
with DAG(
    dag_id='4_1_context_report_dag',
    start_date=datetime(2026, 4, 20),
    schedule_interval=None,
    catchup=False
) as dag:

    # ===================== Generate Context Report Task =====================
    generate_context_report = PythonOperator(
        task_id='generate_context_report',
        python_callable=print_context_report
    )

    # ===================== Generate Context jinja Task =====================
    generate_python_jinja_report = PythonOperator(
        task_id='generate_python_jinja_report',
        python_callable=print_jinja_report,
        op_kwargs={
            'dag_id': '{{ dag.dag_id }}',
            'task_id': '{{ task.task_id }}',
            'run_date': '{{ ds }}',
            'run_id': '{{ run_id }}'
        }
    )
    # ===================== Generate Jinja Report (Bash) =====================
    generate_bash_jinja_report = BashOperator(
        task_id='generate_bash_jinja_report',
        bash_command="""
        echo "====== Pipeline Report ======"
        echo " DAG ID   : {{ dag.dag_id }}"
        echo " Task ID  : {{ task.task_id }}"
        echo " Run Date : {{ ds }}"
        echo " Run ID   : {{ run_id }}"
        echo " Status   : Success"
        echo "============================="
        """
    )
    generate_context_report >> generate_python_jinja_report >> generate_bash_jinja_report