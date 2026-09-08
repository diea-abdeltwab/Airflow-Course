# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator 
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta               
 

# ===================== Default Arguments =====================
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 20),
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='5_2_data_cleaning_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={
        "base_path": "/opt/airflow/dags/data",
        "user_role": "admin",
    }
) as dag:

    # ===================== Sensor: Wait for Input File =====================
    wait_for_sales_file = FileSensor(
        task_id='wait_for_sales_file',
        filepath='dags/data/raw/sales.csv',
        poke_interval=10,
        timeout=120,
        mode='reschedule',
    )
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
    wait_for_sales_file >> extract_and_clean_batch >> generate_report 