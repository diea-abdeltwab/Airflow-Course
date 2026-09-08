# ===================== Import Libraries =====================
from airflow import DAG
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# ===================== Default Arguments =====================
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 20),
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='5_3_sql_sensor_dag',
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as sql_sensor_dag:

    # ===================== SQL Sensor: Wait for Data =====================
    sql_sensor = SqlSensor(
        task_id='sql_sensor',
        conn_id='postgres_conn',
        sql="SELECT 1 FROM customers LIMIT 1;",     # WHERE created_at = CURRENT_DATE
        mode='reschedule',
        poke_interval=5,
        timeout=30,
        soft_fail=True,
    )

    # ===================== Empty Task =====================
    empty_task = EmptyOperator(
        task_id='empty_task',
    )

    # ===================== Task Dependencies =====================
    sql_sensor >> empty_task