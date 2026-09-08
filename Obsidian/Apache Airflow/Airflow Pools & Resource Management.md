
---

> **Pools** limit how many tasks can run **simultaneously** — protecting shared resources.

---

### Creating a Pool

> Pools are managed in the **Airflow UI** → Admin → Pools

```
Pool Name:  limited_pool
Slots:      3
```

Or via CLI:
```bash

docker compose exec -it airflow-webserver bash
 
airflow pools set limited_pool 3 "Limit DB connections"

```

---
#### Pool Parameters

- `pool='default_pool'`: Assign task to a pool
- `pool_slots=1`: Number of slots used by task (default = 1)
- Pools are managed in Airflow UI

📌 **Use case:** Limit resource usage (DB, API, CPU, etc.)

```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

# ===================== Default Arguments =====================
default_args = {
    'pool': 'limited_pool'
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='pool_dag',
    start_date=datetime(2024, 4, 30),
    schedule_interval=None,  
    catchup=False,
    default_args=default_args

) as dag:

    # ===================== Task A =====================
    task_a = BashOperator(
        task_id='task_a',
        bash_command='echo "Running task A"',
        pool_slots=1,
        priority_weight=1
    )

    # ===================== Task B =====================
    task_b = PythonOperator(
        task_id='task_b',
        python_callable=lambda: time.sleep(10),
        pool_slots=1,  # 2
        priority_weight=1
    )

    # ===================== Task C =====================
    task_c = PythonOperator(
        task_id='task_c',
        python_callable=lambda: time.sleep(10),
        pool_slots=1,  # 2
        priority_weight=1
    )

    # ===================== Task D =====================
    task_d = PythonOperator(
        task_id='task_d',
        python_callable=lambda: time.sleep(10),
        pool_slots=1,
        priority_weight=1
    )

    # ===================== Task E =====================
    task_e = PythonOperator(
        task_id='task_e',
        python_callable=lambda: time.sleep(10),
        pool_slots=1,
        priority_weight=1  # 2
    )

    # ===================== Task Dependencies =====================
    task_a >> [task_b, task_c, task_d, task_e]
```

---


![[Pasted image 20260502045801.png]]

---