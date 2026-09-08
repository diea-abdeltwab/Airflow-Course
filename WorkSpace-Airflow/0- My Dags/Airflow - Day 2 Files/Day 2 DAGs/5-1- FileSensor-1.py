# ===================== Import Libraries =====================
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# ===================== Default Arguments =====================
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 20),
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='5_1_file_sensor_dag',
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as file_sensor_dag:

    # ===================== File Sensor Task =====================
    file_sensor = FileSensor(
        task_id='file_sensor',
        filepath='dags/data/raw/sales.csv', 
        fs_conn_id='fs_default',  
        mode='poke',                           # reschedule
        poke_interval=5,
        timeout=30,
        #soft_fail=True,
    )

    # ===================== Empty Task =====================
    empty_task = EmptyOperator(
        task_id='empty_task',
    )

    # ===================== Task Dependencies =====================
    file_sensor >> empty_task


