

---
---
---

## Python Connection

### SQLAlchemy

#### Key Parameters

- `create_engine()`: Create database connection (unified interface)
- `'postgresql://user:pass@host:port/db'`: Connection URL (DB-specific)
- `engine.connect()`: Open connection
- `conn.execute()`: Execute SQL statements
- `with engine.connect()`: Auto close connection
- No Airflow `conn_id` (manual connection)

📌 **Use case:** Unified way to interact with multiple databases (Postgres / MySQL / SQLite / etc.)

```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine


# ===================== Custom SQL Function =====================
def run_custom_sql():
    engine = create_engine(
        "postgresql://myuser:123@my-postgres:5432/mydb"
    )
    with engine.connect() as conn:
        records = conn.execute("SELECT * FROM customers LIMIT 5;") 
        for row in records:           # List of Tuples
            print(row)


# ===================== Default Args =====================
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 20),
}


# ===================== DAG Definition =====================
with DAG(
    dag_id='8_1_sqlalchemy_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,

) as dag:
    # ===================== Python Operator Task ===================== 
    custom_sql_task = PythonOperator(
        task_id='custom_sql',
        python_callable=run_custom_sql
    )
```


---
---
---

## Airflow Hooks

> A **Hook** is a low-level interface to **connect directly** to an external service.
> Operators use Hooks internally — but you can use them directly for custom logic.


![[Pasted image 20260502053925.png]]

---

### Hook vs Operator

| | Hook | Operator |
|--|------|----------|
| **Level** | Low-level | High-level |
| **Use** | Custom logic | Standard task |
| **Example** | `PostgresHook` | `PostgresOperator` |

> ✅ Use **Operator** when it does exactly what you need.
> ✅ Use **Hook** when you need custom logic the Operator can't do.

---

### PostgresHook

> Connect directly to PostgreSQL and run custom queries.

#### Key Parameters

- `postgres_conn_id='postgres_conn'`: Connection ID from Airflow
- `hook.get_conn()`: Open a raw DB connection

- `hook.get_records(sql)`: Run SELECT and get rows
- `hook.run(sql)`: Run INSERT / UPDATE / DELETE

```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# ===================== Query Data Using Postgres Hook =====================
def query_with_hook():
    hook = PostgresHook(postgres_conn_id='postgres_conn')

    records = hook.get_records("SELECT * FROM customers LIMIT 5;") # List of Tuples

    for row in records:
        print(row)

# ===================== Insert Data Using Postgres Hook =====================
def insert_with_hook():
    hook = PostgresHook(postgres_conn_id='postgres_conn')

    hook.run("""
        INSERT INTO customers (customer_id, customer_name, address, birth_date)
        VALUES ('C999', 'Test User', 'Cairo', '2000-01-01')
        ON CONFLICT (customer_id) DO NOTHING;
    """)
    print("Row inserted successfully")

# ===================== Get Count Using Postgres Hook =====================
def get_count_with_hook():
    hook = PostgresHook(postgres_conn_id='postgres_conn')

    result = hook.get_first("SELECT COUNT(*) FROM customers;")  # Tuple
    print(f"Total customers: {result[0]}")                      # (100,)

# ===================== DAG Definition =====================
with DAG(
    dag_id='8_2_hook_dag',
    start_date=datetime(2026, 4, 20),
    schedule_interval=None,
    catchup=False,
) as dag:

    # ===================== Query Records Task =====================
    get_records = PythonOperator(
        task_id='query_with_hook',
        python_callable=query_with_hook,
    )

    # ===================== Insert Record Task =====================
    insert = PythonOperator(
        task_id='insert_with_hook',
        python_callable=insert_with_hook,
    )

    # ===================== Count Records Task =====================
    count = PythonOperator(
        task_id='get_count',
        python_callable=get_count_with_hook,
    )

    # ===================== Task Dependencies =====================
    get_records >> insert >> count
```

---
### Common Hooks

| Hook              | Provider                                | Use Case             |
| ----------------- | --------------------------------------- | -------------------- |
| `PostgresHook`    | `apache-airflow-providers-postgres`     | PostgreSQL queries   |
| `MySqlHook`       | `apache-airflow-providers-mysql`        | MySQL queries        |
| `HttpHook`        | `apache-airflow-providers-http`         | REST APIs            |
| `S3Hook`          | `apache-airflow-providers-amazon`       | AWS S3 files         |
| `GCSHook`         | `apache-airflow-providers-google`       | Google Cloud Storage |
| `SlackHook`       | `apache-airflow-providers-slack`        | Slack messages       |
| `SparkSubmitHook` | `apache-airflow-providers-apache-spark` | Spark  jobs          |

---