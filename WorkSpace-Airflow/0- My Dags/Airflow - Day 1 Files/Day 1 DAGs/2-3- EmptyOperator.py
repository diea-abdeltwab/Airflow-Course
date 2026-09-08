# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# ===================== Default Arguments =====================
dag_default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 20),
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='2_3_empty_operator_dag',
    catchup=False,
    schedule_interval=None,
    default_args=dag_default_args
) as empty_operator_dag:

    # ===================== Start Task =====================
    start_task = EmptyOperator(
        task_id='start'
    )

    # ===================== Middle Task =====================
    middle_task = EmptyOperator(
        task_id='task'
    )

    # ===================== End Task =====================
    end_task = EmptyOperator(
        task_id='end'
    )

    # ===================== Task Dependencies =====================
    start_task >> middle_task >> end_task