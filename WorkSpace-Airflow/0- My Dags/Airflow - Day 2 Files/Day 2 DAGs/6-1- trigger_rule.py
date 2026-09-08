# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# ===================== Default Arguments =====================
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 20),
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='6_1_trigger_rule_demo',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # ===================== Source Task A =====================
    source_a = BashOperator(
        task_id='source_a',
        bash_command='echo "A done"'
    )

    # ===================== Source Task B =====================
    source_b = BashOperator(
        task_id='source_b',
        bash_command='exit 1'  # intentionally fails
    )

    # ===================== Merge Task with Trigger Rule =====================
    merge_task = EmptyOperator(
        task_id='merge_task',
        trigger_rule='all_success'  # one_success
    )

    # ===================== Task Dependencies =====================
    [source_a, source_b] >> merge_task