# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json


# ===================== Push (Send Data via XCom) =====================
def push_data(ti):
    data_1 = {'rows': 1500, 'status': 'ok'}
    data_2 = {'rows': 2000, 'status': 'Not ok'}
    ti.xcom_push(key='extract_data_1', value=data_1)
    ti.xcom_push(key='extract_data_2', value=data_2)


# ===================== Pull (Receive Data via XCom) =====================
def pull_data(ti):
    pull_data_1 = ti.xcom_pull(key='extract_data_1')
    pull_data_2 = ti.xcom_pull(key='extract_data_2')
    print(f"Got Data 1: {pull_data_1['rows']} rows with status: {pull_data_1['status']}")
    print(f"Got Data 2: {pull_data_2['rows']} rows with status: {pull_data_2['status']}")


# ===================== DAG Definition =====================
with DAG(
    dag_id='7_2_xcom_manual_dag',
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