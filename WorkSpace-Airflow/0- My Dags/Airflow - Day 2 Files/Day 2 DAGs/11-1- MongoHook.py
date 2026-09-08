# ===================== Import Libraries =====================
from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from datetime import datetime

# ===================== Tasks =====================

# Create Collection
def create_collection():
    hook = MongoHook(mongo_conn_id='mongo_conn')
    client = hook.get_conn()
    db = client['mydb']
    if 'sales' not in db.list_collection_names():
        db.create_collection('sales')
        print("Collection 'sales' created")
    else:
        print("Collection 'sales' already exists")


# Insert Documents
def insert_documents():
    hook = MongoHook(mongo_conn_id='mongo_conn')
    client = hook.get_conn()
    db = client['mydb']
    docs = [
        {"customer": "Ahmed",   "amount": 500, "city": "Cairo"},
        {"customer": "Sara",    "amount": 300, "city": "Alex"},
        {"customer": "Mohamed", "amount": 750, "city": "Giza"},
    ]
    result = db['sales'].insert_many(docs)
    print(f"Inserted {len(result.inserted_ids)} documents")


# Read Documents with Condition
def read_documents():
    hook = MongoHook(mongo_conn_id='mongo_conn')
    client = hook.get_conn()
    db = client['mydb']
    docs = list(
        db['sales'].find(
            {"amount": {"$gt": 400}},     # amount > 400
            {"_id": 0}
        )
    )
    print("===== Documents where amount > 400 =====")
    for doc in docs:
        print(doc)


# ===================== DAG Definition =====================
with DAG(
    dag_id='mongo_hook_dag',
    start_date=datetime(2026, 4, 20),
    schedule_interval=None,
    catchup=False,
) as dag:

    create_collection_task = PythonOperator(
        task_id='create_collection',
        python_callable=create_collection
    )

    insert_documents_task = PythonOperator(
        task_id='insert_documents',
        python_callable=insert_documents
    )

    read_documents_task = PythonOperator(
        task_id='read_documents',
        python_callable=read_documents
    )

    # ===================== Dependencies =====================
    create_collection_task >> insert_documents_task >> read_documents_task