# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import logging


# ===================== Callbacks =====================

# ===================== DAG Failure Callback =====================
def log_dag_failure(context):
    logging.error(f"DAG {context['dag'].dag_id} failed ❌")


# ===================== Task Success Callback =====================
def log_task_success(context):
    logging.info(f"Task {context['task_instance'].task_id} succeeded ✅")


# ===================== Task Failure Callback =====================
def log_task_failure(context):
    logging.error(f"Task {context['task_instance'].task_id} failed ❌")


# ===================== Default Arguments =====================
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 20),
}


# ===================== DAG Definition =====================
with DAG(
    dag_id="6_6_task_callback_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    on_failure_callback=log_dag_failure
) as dag:

    # ===================== Task: Successful Execution =====================
    print_message_task = BashOperator(
        task_id="print_message",
        bash_command="echo 'Hello Airflow'",

        on_success_callback=log_task_success,
        on_failure_callback=log_task_failure
    )

    # ===================== Task: Intentional Failure =====================
    failing_task = BashOperator(
        task_id="failing_task",
        bash_command="exit 1",

        on_success_callback=log_task_success,
        on_failure_callback=log_task_failure
    )

    # ===================== Task Dependencies =====================
    print_message_task >> failing_task