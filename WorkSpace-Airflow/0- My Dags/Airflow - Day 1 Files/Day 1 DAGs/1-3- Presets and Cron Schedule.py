# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# ===================== Default Arguments =====================
dag_default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 5, 7), 
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='1_3_trigger_dag',
    description='My First DAG',
    tags=['print_message'],
    catchup=False,
    schedule_interval='@hourly',                     #'30 12 * * 3'
    default_args=dag_default_args
) as simple_dag:

    # ===================== Define Task =====================
    print_message_task = BashOperator(
        task_id='print_message',
        bash_command='echo "Hello World!"'
    )