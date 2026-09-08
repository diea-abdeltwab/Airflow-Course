# ===================== Import Libraries =====================
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime


# ===================== Python Function =====================
def check_data_availability():
    data_exists = False  # your real check here

    if data_exists:
        print("Data exists. Proceeding with processing...")
    else:   
        raise AirflowSkipException("No data today — skipping downstream tasks")



# ===================== Default Arguments =====================
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 20),
    'retries': 1,
}


# ===================== DAG Definition =====================
with DAG(
    dag_id='6_3_data_availability_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # ===================== Check Data Availability Task =====================
    check_data_task = PythonOperator(
        task_id='check_data_availability',
        python_callable=check_data_availability
    )

    # ===================== Process Sales Data Task =====================
    process_sales_data = BashOperator(
        task_id='process_sales_data',
        bash_command='echo "Processing sales data..."'
    )

    # ===================== Process Orders Data Task =====================
    process_orders_data = BashOperator(
        task_id='process_orders_data',
        bash_command='echo "Processing orders data..."',
    )

    # ===================== Task Dependencies =====================
    check_data_task >> [process_sales_data, process_orders_data]