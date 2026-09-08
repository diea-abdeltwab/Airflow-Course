# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# ===================== Default Arguments =====================
dag_default_args = {
    'owner': 'Diea',
    'start_date': datetime(2026, 4, 20),
    'retries': 3,
    'retry_delay': 5
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='1_2_manual_trigger_dag',
    description='My First DAG',
    tags=['print_message' , 'manual_trigger'],
    catchup=False,
    #schedule_interval=None,
    default_args=dag_default_args
) as simple_dag:

    # ===================== Define Task =====================
    print_message_task = BashOperator(
        task_id='print_message',
        bash_command='echo "Hello World!"' #'sleep 3; exit 1'
        
    )