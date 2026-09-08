# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

# ===================== Python Function =====================
def python_task(**context):
    print("====== Python Task ======")
    print(f"User Name : {context['params']['user_name']}")
    print(f"File Name : {context['params']['file_name']}")
    print(f"Separator  : {context['params']['separator']}")
    print("=========================")

# ===================== DAG =====================
with DAG(
    dag_id='4_3_params_mixed_example',
    start_date=datetime(2026, 4, 20),
    schedule_interval=None,
    catchup=False,
    params={
        "user_name": "Diea",
        "file_name": "sales.csv"
    }
) as dag:

    task_python = PythonOperator(
        task_id='python_task',
        python_callable=python_task,
        params={"separator": ","}
    )

    task_bash = BashOperator(
        task_id='bash_task',
        bash_command="""
        echo "====== Bash Task ======"
        echo "User Name: {{ params.user_name }}"
        echo "File Name: {{ params.file_name }}"
        echo "Separator : {{ params.separator }}"
        echo "======================="
        """,
        params={"separator": ";"}
    )

    task_python >> task_bash