# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
import logging
from airflow.utils.email import send_email


# ===================== SLA Miss Callback Function =====================
def handle_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):

    subject = f"⚠️ Airflow SLA Miss: {dag.dag_id}"
    
    html_content = f"""
    <h3>SLA Miss Detected!</h3>
    <p><b>DAG:</b> {dag.dag_id}</p>
    <p><b>Tasks Affected:</b> {task_list}</p>
    <p><b>Blocking Tasks:</b> {blocking_task_list}</p>
    <p><b>Time:</b> {datetime.now()}</p>
    """
    
    send_email(
        to=['tempm7513@gmail.com'],
        subject=subject,
        html_content=html_content
    )

    logging.warning(f"❌ SLA Email sent for DAG={dag.dag_id}")


# ===================== Default Arguments =====================
default_args = {
    'start_date': datetime(2024, 4, 30),
    'sla': timedelta(seconds=5),
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='sla_miss_callback_email_dag',
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