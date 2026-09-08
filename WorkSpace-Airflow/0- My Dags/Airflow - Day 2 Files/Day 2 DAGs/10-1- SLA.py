# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
import logging


# ===================== Default Arguments =====================
default_args = {
    'start_date': datetime(2024, 4, 30)
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='sla_dag',
    default_args=default_args,
    schedule_interval='* * * * *',  # Run every minute
    catchup=False,
    
) as dag:

    # ===================== Fast Task (Meets SLA) =====================
    fast_task = PythonOperator(
        task_id='fast_task',
        python_callable=lambda: print("Done quickly!"),
        sla=timedelta(seconds=5),
        
    )

    # ===================== Slow Task (SLA Miss Example) =====================
    slow_task = PythonOperator(
        task_id='slow_task',
        python_callable=lambda: time.sleep(10),
        sla=timedelta(seconds=5), #execution_timeout=timedelta(seconds=5)
    )

    # ===================== Task Dependencies =====================
    fast_task >> slow_task