# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

# ===================== Branch Decision Function =====================
def check_data_quality(data_is_valid):
    # run some checks on the data
    if data_is_valid:
        return 'run_etl'
    else:
        return 'send_alert'

# ===================== DAG Definition =====================
with DAG(
    dag_id='6_2_branching_pipeline',
    start_date=datetime(2026, 4, 20),
    schedule_interval=None,
    catchup=False
) as dag:

    # ===================== Branch Decision Task =====================
    branch = BranchPythonOperator(
        task_id='check_quality',
        python_callable=check_data_quality,
        op_kwargs={'data_is_valid': False}
    )

    # ===================== Path A: Good Data =====================
    run_etl = BashOperator(
        task_id='run_etl',
        bash_command='echo "Running ETL..."'
    )

    # ===================== Path B: Bad Data =====================
    send_alert = BashOperator(
        task_id='send_alert',
        bash_command='echo "Data quality issue!"'
    )

    # ===================== Final Task =====================
    final_status = BashOperator(
        task_id='final_status',
        bash_command='echo "Pipeline finished regardless of data quality."',
        # trigger_rule='none_failed'
    )

    # ===================== Task Dependencies =====================
    branch >> [run_etl, send_alert] >> final_status