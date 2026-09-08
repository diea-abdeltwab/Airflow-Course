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
    dag_id='7_4_xcom_json_dag',
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