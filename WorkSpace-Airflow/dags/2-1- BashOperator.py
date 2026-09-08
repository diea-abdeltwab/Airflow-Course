# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# ===================== Default Arguments =====================
dag_default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 20),
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='2_1_bash_operator_dag',
    catchup=False,
    schedule_interval=None,
    default_args=dag_default_args,
) as bash_operator_dag:

    # ===================== Task 1: Print Message =====================
    print_message_task = BashOperator(
        task_id='print_message',
        bash_command='echo "Hello from (print_message)"',
    )

    # ===================== Task 2: Run Shell Script =====================
    run_script_task = BashOperator(
        task_id='run_script',
        bash_command='./scripts/print_message_script.sh',
    )

    # ===================== Task 3: Run Script with env =====================
    run_script_with_env_task = BashOperator(
        task_id='run_script_with_env',
        bash_command='bash /opt/airflow/dags/scripts/print_message_with_env_script.sh ',
        env={
            'MY_NAME': 'Airflow',
            'TASK_NAME': 'run_script_with_env_task',
        },
    )

    # ===================== Define Task Dependencies =====================
    print_message_task >> run_script_task >> run_script_with_env_task