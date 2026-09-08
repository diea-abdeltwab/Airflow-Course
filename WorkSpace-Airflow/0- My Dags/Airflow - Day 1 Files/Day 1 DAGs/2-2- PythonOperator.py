# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime


# ===================== Python Functions =====================

def print_hello():
    print("Hello from PythonOperator!")


def print_hello_with_args (my_name, task_name):
    print(f"Hello {my_name}, from {task_name}")


# ===================== Default Arguments =====================
dag_default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 20),
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='2_2_python_dag',
    catchup=False,
    schedule_interval=None,
    default_args=dag_default_args
) as python_operator_dag:

    # ===================== Task 1: Simple Python Function =====================
    print_message_task = PythonOperator(
        task_id='print_message',
        python_callable=print_hello
    )

    #=================== Task 2: Function with Positional Args ==================
    print_message_with_args_task = PythonOperator(
        task_id='print_message_with_env',
        python_callable=print_hello_with_args,
        op_args=['Airflow', 'print_message_with_env']
    )

    #===================== Task 3: Function with Keyword Args ===================
    print_message_with_kwargs_task = PythonOperator(
        task_id='print_message_with_env_2',
        python_callable=print_hello_with_args,
        op_kwargs={
            'my_name': 'Airflow',
            'task_name': 'print_message_with_env_2'
        }
    )

    #===================== Task 4: Run External Python Script ===================
    run_python_script_task = BashOperator(
        task_id='print_message_bash',
        bash_command='python /opt/airflow/dags/scripts/print_message_py_script.py'
    )

    # ===================== Define Task Dependencies =====================
    print_message_task >> print_message_with_args_task >> print_message_with_kwargs_task >> run_python_script_task