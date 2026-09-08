
---
## MongoDB

---

#### Connection Setup

```
AIRFLOW_CONN_MONGO_CONN=mongodb://myuser:123@my-mongodb:27017/mydb?authSource=admin
```

---

### MongoCustomQueryOperator

> Run custom queries on MongoDB from Airflow.

#### Key Parameters

- `task_id`: Unique task name
- `mongo_conn_id='mongo_default'`: MongoDB connection ID
- `database`: Database name
- `collection`: Collection name
- `query`: MongoDB query (filter)
- `projection`: Fields to return (optional)
- `operation`: Type of operation (`find`, `insert`, `update`, `delete`)

```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.providers.mongo.operators.mongo import MongoCustomQueryOperator
from datetime import datetime

# ===================== DAG Definition =====================
with DAG(
    dag_id='mongo_crud_dag',
    start_date=datetime(2026, 4, 20),
    schedule_interval=None,
    catchup=False,
) as dag:

    # ===================== 1. Create Collection =====================
    create_collection = MongoCustomQueryOperator(
        task_id='create_collection',
        mongo_conn_id='mongo_default',
        database='mydb',
        collection='sales',
        query={},
        operation='create'
    )

    # ===================== 2. Insert Documents =====================
    insert_documents = MongoCustomQueryOperator(
        task_id='insert_documents',
        mongo_conn_id='mongo_default',
        database='mydb',
        collection='sales',
        query=[
            {"customer": "Ahmed",   "amount": 500, "city": "Cairo"},
            {"customer": "Sara",    "amount": 300, "city": "Alex"},
            {"customer": "Mohamed", "amount": 750, "city": "Giza"},
        ],
        operation='insert_many'
    )

    # ===================== 3. Read Documents =====================
    read_documents = MongoCustomQueryOperator(
        task_id='read_documents',
        mongo_conn_id='mongo_default',
        database='mydb',
        collection='sales',
        query={},
        operation='find'
    )

    create_collection >> insert_documents >> read_documents   
```

---
### MongoHook

> Used to connect and interact with MongoDB from Airflow.

#### Key Parameters

- `mongo_conn_id='mongo_default'`: MongoDB connection ID
- `conn`: Returns the MongoDB client
- `get_collection`: Get a collection from the database

```python
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
            {"amount": {"$gt": 400}},  # amount > 400
            {"_id": 0}
        )
    )
    print("===== Documents where amount > 400 =====")
    for doc in docs
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
```


---

### MongoSensor

> Used to wait until a specific document or condition exists in MongoDB before continuing the DAG.

#### Key Parameters

- `mongo_conn_id='mongo_conn'`: MongoDB connection ID
- `mongo_db='mydb'`: Database name
- `collection='sales'`: Collection name
- `query={"customer": "Omar"}`: MongoDB query condition
- `poke_interval=5`: Time between checks (seconds)
- `timeout=60`: Maximum waiting time before failing

```python
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
```
---
---
---
