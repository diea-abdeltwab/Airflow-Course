

---

## SLA (Service Level Agreement)

> An **SLA** in Airflow defines the **maximum allowed time** for a task or DAG to complete.
> If exceeded: Airflow **alerts** but does **NOT** stop the task.


![[Pasted image 20260502031851.png]]

---

### SLA 

```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
import logging


# ===================== Default Arguments =====================
default_args = {
    'start_date': datetime(2024, 4, 30),
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
```


---

### SLA Miss Callback

#### Key Behaviors

-  **SLA Miss Alert**: Airflow sends a notification (Email / Slack / etc.)
-  **Logging Event**: SLA miss is recorded in task logs
-  **UI Indicator**: Task is marked as _SLA Missed_ in Airflow UI
-  **Optional Callback**: Custom function can be triggered using `sla_miss_callback`

📌 **Important:** SLA does NOT fail or stop the task, it only triggers monitoring actions.

```python
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
    dag_id='sla_dag',
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
```

---

```python
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
        to=['your-email@example.com'],
        subject=subject,
        html_content=html_content
    )

    logging.warning(f"❌ SLA Email sent for DAG={dag.dag_id}")


# ===================== Default Arguments =====================
default_args = {
    'start_date': datetime(2024, 4, 30),
    'sla': timedelta(seconds=5),
    'email': ['your-email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='sla_dag',
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
```

---
---
---
