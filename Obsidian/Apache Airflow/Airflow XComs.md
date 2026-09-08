

---

> **XCom** (Cross-Communication) lets tasks **share data** with each other inside a DAG.

---

![[Pasted image 20260502024623.png]]

---

### XCom Basics

#### Key Parameters

- `key='message'`: Identifier for stored value
- `value='data'`: Data to store
- `task_ids='task1'`: Source task to pull from
- `ti.xcom_push()`: Send data to XCom
- `ti.xcom_pull()`: Retrieve data from XCom

- Data is stored in Airflow metadata DB
📌 **Use case:** Share data between tasks inside DAG


#### Push & Pull Methods

| Method | Direction | How |
|--------|-----------|-----|
| `ti.xcom_push(key, value)` | Send data | Manual |
| `return value` | Send data | Auto (implicit push) |
| `ti.xcom_pull(key, task_ids)` | Receive data | Manual |

#### Auto Push

```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json


# ===================== Push (Send Data via XCom) =====================
def push_data(ti):
    result = {'rows': 1500, 'status': 'ok'}
    return result


# ===================== Pull (Receive Data via XCom) =====================
def pull_data(ti):
    data = ti.xcom_pull()
    print(f"Got {data['rows']} rows with status: {data['status']}")


# ===================== DAG Definition =====================
with DAG(
    dag_id='xcom_auto_dag',
    start_date=datetime(2026, 4, 20),
    schedule_interval=None,
    catchup=False
) as dag:

    # ===================== Push Task =====================
    push = PythonOperator(
        task_id='push',
        python_callable=push_data
    )

    # ===================== Pull Task =====================
    pull = PythonOperator(
        task_id='pull',
        python_callable=pull_data
    )
	
    # ===================== Task Dependencies =====================
    push >> pull
```

---
#### Manual Push

```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json


# ===================== Push (Send Data via XCom) =====================
def push_data(ti):
    result = {'rows': 1500, 'status': 'ok'}
    ti.xcom_push(key='extract_result', value=result)


# ===================== Pull (Receive Data via XCom) =====================
def pull_data(ti):
    data = ti.xcom_pull(key='extract_result')  # task_ids='push'
    print(f"Got {data['rows']} rows with status: {data['status']}")


# ===================== DAG Definition =====================
with DAG(
    dag_id='xcom_manual_dag',
    start_date=datetime(2026, 4, 20),
    schedule_interval=None,
    catchup=False
) as dag:

    # ===================== Push Task =====================
    push = PythonOperator(task_id='push', python_callable=push_data)

    # ===================== Pull Task =====================
    pull = PythonOperator(task_id='pull', python_callable=pull_data)
	
    # ===================== Task Dependencies =====================
    push >> pull
```

----

```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json


# ===================== Push (Send Data via XCom) =====================
def push_data_1(ti):
    data_1 = {'rows': 1500, 'status': 'ok'}
    ti.xcom_push(key='extract_data_1', value=data_1)

def push_data_2(ti):
    data_2 = {'rows': 2000, 'status': 'Not ok'}
    ti.xcom_push(key='extract_data_1', value=data_2)



# ===================== Pull (Receive Data via XCom) =====================
def pull_data(ti):
    pull_data_1 = ti.xcom_pull(key='extract_data_1' , task_ids='push_1')
    pull_data_2 = ti.xcom_pull(key='extract_data_1', task_ids='push_2')
    print(f"Got Data 1: {pull_data_1['rows']} rows with status: {pull_data_1['status']}")
    print(f"Got Data 2: {pull_data_2['rows']} rows with status: {pull_data_2['status']}")


# ===================== DAG Definition =====================
with DAG(
    dag_id='7_3_xcom_manual_dag',
    start_date=datetime(2026, 4, 20),
    schedule_interval=None,
    catchup=False
) as dag:

    # ===================== Push Task =====================
    push_1 = PythonOperator(task_id='push_1', python_callable=push_data_1)

    push_2 = PythonOperator(task_id='push_2', python_callable=push_data_2)

    # ===================== Pull Task =====================
    pull = PythonOperator(task_id='pull', python_callable=pull_data)
	
    # ===================== Task Dependencies =====================
    push_1 >> push_2 >> pull
```

---

### Passing Lists/Dicts — JSON Serialization

> XCom only stores **serializable** data. For complex objects, use `json.dumps()`.
#### Solution (JSON Serialization)

- `json.dumps()` → convert object to string
- `json.loads()` → convert string back to object

📌 **Use case:** Safely pass structured data (lists/dicts) between tasks using XCom

```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json


# ===================== Push (Send Data via XCom) =====================
def push_data(ti):
    records = [
        {'id': 1, 'name': 'Ahmed',   'amount': 500},
        {'id': 2, 'name': 'Sara',    'amount': 750},
        {'id': 3, 'name': 'Youssef', 'amount': 300},
    ]
    ti.xcom_push(key='daily_records', value=json.dumps(records))


# ===================== Pull (Receive & Process Data) =====================
def pull_data(ti):
    raw = ti.xcom_pull(key='daily_records', task_ids='push')
    records = json.loads(raw)

    total = sum(r['amount'] for r in records)
    print(f"Total amount: {total}")
    for r in records:
        print(f"  {r['name']}: {r['amount']}")


# ===================== DAG Definition =====================
with DAG(
    dag_id='xcom_json_dag',
    start_date=datetime(2026, 4, 20),
    schedule_interval=None,
    catchup=False
) as dag:

    # ===================== Push Task =====================
    push = PythonOperator(task_id='push', python_callable=push_data)

    # ===================== Pull Task =====================
    pull = PythonOperator(task_id='pull', python_callable=pull_data)
	
    # ===================== Task Dependencies =====================
    push >> pull  
```

----
