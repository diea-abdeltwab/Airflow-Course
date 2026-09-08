# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator 
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta               
 

# ===================== Default Arguments =====================
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 20),
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='4_4_data_cleaning_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={
        "base_path": "/opt/airflow/dags/data",
        "user_role": "admin",
    }
) as dag:

    # ===================== Task 1: Extract & Clean =====================
    extract_and_clean_batch = BashOperator(
        task_id='extract_and_clean_batch',
        bash_command= """
                echo "Processing {{ params.input_file  }}..."
                python /opt/airflow/dags/scripts/cleandata.py \
                {{ ds_nodash }} \
                {{ params.base_path }}/raw/{{ params.input_file }} \
                {{ params.base_path }}/cleaned
                """,
        params={
            'input_file': 'sales.csv'
        }
    )

    # ===================== Generate Report =====================
    generate_report = BashOperator(
        task_id='generate_report',
        bash_command="""
            echo "Pipeline Run Date: {{ ds_nodash }}" && \
            echo "DAG: {{ dag.dag_id }}" && \
            echo "Output Folder: {{ params.base_path }}/cleaned" && \
            echo "Executed By: {{ params.user_role }}"
        """
    )
    # ===================== Task Dependencies =====================
    extract_and_clean_batch >> generate_report 