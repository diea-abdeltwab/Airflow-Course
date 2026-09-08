# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
import logging


'''
# ===================== Alternative SLA Miss Handler =====================
def handle_sla_miss():
    print("❌ SLA missed! Taking action...")
'''


# ===================== SLA Miss Callback Function =====================
def handle_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    for sla in slas:
        logging.warning(f"❌ SLA missed: DAG={sla.dag_id} | Task={sla.task_id}")


# ===================== Default Arguments =====================
default_args = {
    'start_date': datetime(2024, 4, 30),
    'sla': timedelta(seconds=5)  
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='sla_miss_callback_dag',
    default_args=default_args,
    schedule_interval='* * * * *',  # Run every minute
    catchup=False,
    sla_miss_callback=handle_sla_miss,
    
) as dag:

    # ===================== Fast Task =====================
    fast_task = PythonOperator(
        task_id='fast_task',
        python_callable=lambda: print("Done quickly!"),
        
    )

    # ===================== Slow Task (Triggers SLA Miss) =====================
    slow_task = PythonOperator(
        task_id='slow_task',
        python_callable=lambda: time.sleep(10),
    )

    # ===================== Task Dependencies =====================
    fast_task >> slow_task