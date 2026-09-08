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
    print(f"data : {data}")


# ===================== DAG Definition =====================
with DAG(
    dag_id='7_1_xcom_auto_dag',
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