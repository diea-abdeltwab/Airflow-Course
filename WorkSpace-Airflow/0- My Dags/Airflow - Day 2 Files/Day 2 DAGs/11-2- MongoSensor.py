# ===================== Import Libraries =====================
from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.mongo.sensors.mongo import MongoSensor
from airflow.operators.python import PythonOperator
from datetime import datetime

# ===================== Insert Document =====================
def insert_document():
    hook = MongoHook(mongo_conn_id='mongo_conn')
    client = hook.get_conn()
    db = client['mydb']

    db['sales'].insert_one({
        "customer": "Omar",
        "amount": 900,
        "city": "Fayoum"
    })

    print("Document Inserted")


# ===================== Delete Document =====================
def delete_document():
    hook = MongoHook(mongo_conn_id='mongo_conn')
    client = hook.get_conn()
    db = client['mydb']

    result = db['sales'].delete_one({
        "customer": "Omar"
    })

    print(f"Deleted {result.deleted_count} document")


# ===================== DAG =====================
with DAG(
    dag_id='mongo_sensor_dag',
    start_date=datetime(2026, 4, 20),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Insert Task
    insert_task = PythonOperator(
        task_id='insert_document',
        python_callable=insert_document
    )

    # Mongo Sensor
    wait_for_document = MongoSensor(
        task_id='wait_for_document',
        mongo_conn_id='mongo_conn',
        mongo_db='mydb',
        collection='sales',
        query={
            "customer": "Omar"
        },
        poke_interval=5,
        timeout=30
    )

    # Delete Task
    delete_task = PythonOperator(
        task_id='delete_document',
        python_callable=delete_document
    )

    # ===================== Dependencies =====================
    wait_for_document >> insert_task >> delete_task