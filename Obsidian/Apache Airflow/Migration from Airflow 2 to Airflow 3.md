

---

> **Airflow 3** is a major release with breaking changes.
> This note covers the key differences and what to update.

---

### What's New in Airflow 3

| Area | Change |
|------|--------|
| **DAG Versioning** | Airflow tracks DAG versions — old runs show old code |
| **Asset-Driven Scheduling** | DAGs can trigger based on data, not just time |
| **Backfill UI** | Run backfills directly from the UI |
| **New UI** | Completely redesigned interface |
| **Task Execution API** | Workers communicate via API, not DB |
| **DAG Processor** | Separate process — no longer part of Scheduler |
| **Python Support** | Minimum Python 3.9 |

---

### Breaking Changes

---

#### 1. Removed `schedule_interval` — use `schedule`

```python
# ❌ Airflow 2
with DAG(
    dag_id='my_dag',
    schedule_interval='@daily',
    ...
) as dag:
    pass

#===========================================================================

# ✅ Airflow 3
with DAG(
    dag_id='my_dag',
    schedule='@daily',
    ...
) as dag:
    pass
```

---

#### 2. `start_date` moved out of `default_args`

```python
# ❌ Airflow 2 — start_date in default_args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG(dag_id='my_dag', default_args=default_args) as dag:
    pass

#===========================================================================

# ✅ Airflow 3 — start_date directly in DAG
default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='my_dag',
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
) as dag:
    pass
```

---

#### 3. `execution_date` replaced by `logical_date`

```python
# ❌ Airflow 2
def my_task(**context):
    date = context['execution_date']
    ds   = context['ds']

#===========================================================================

# ✅ Airflow 3
def my_task(**context):
    date = context['logical_date']
    ds   = context['ds']              
```

---

#### 4. `PostgresOperator` import changed

```python
# ❌ Airflow 2
from airflow.operators.postgres_operator import PostgresOperator

#===========================================================================

# ✅ Airflow 3
from airflow.providers.postgres.operators.postgres import PostgresOperator
```

---

#### 5. `provide_context` removed from PythonOperator

```python
# ❌ Airflow 2
task = PythonOperator(
    task_id='my_task',
    python_callable=my_func,
    provide_context=True,         # needed in Airflow 2
)

#===========================================================================

# ✅ Airflow 3 — context is always passed via **context
task = PythonOperator(
    task_id='my_task',
    python_callable=my_func,      # just add **context to the function
)
```

---

#### 6. Replace `Dataset` with `Asset`

>In Airflow 3, the concept of **Datasets** has been renamed to **Assets**. This change reflects a broader scope, as an "Asset" can represent a table, a file, or even an ML model.

```python
# ❌ Airflow 2
from airflow.datasets import Dataset
my_data = Dataset("s3://bucket/file.csv")

# ✅ Airflow 3
from airflow.assets import Asset
my_data = Asset("s3://bucket/file.csv")
```


![[Pasted image 20260502124410.png]]


---
#### 7. Task Execution API (Architectural Shift)

>A major change for DevOps and Infrastructure: In Airflow 3, Workers no longer communicate directly with the Meta Database. Instead, they communicate via a dedicated **Task Execution API**.

- **Benefit:** Enhanced security and easier scaling of workers in environments like Kubernetes.

---
#### 9. DAG Processor Separation

> The **DAG Processor** is now a completely separate process from the Scheduler.

- **Why it matters:** If a DAG has heavy parsing logic or an infinite loop, it will only affect the DAG Processor, ensuring the **Scheduler** remains stable and continues to trigger other tasks.

---

![[Pasted image 20260502124459.png]]

---
---
---
