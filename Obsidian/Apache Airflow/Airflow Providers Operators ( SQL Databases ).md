 

---

> Pre-built tools that let Airflow interact with **external systems** like Databases, Cloud services, and Data Platforms

---
---
---

## Connection Setup

> Before using any Providers operator, you need to configure a **Connection** in Airflow.

#### 1. Environment Variable (Best for Docker/K8s)

```bash

# In docker-compose.yml or .env:

AIRFLOW_CONN_POSTGRES_CONN=postgresql://user:pass@host:5432/mydb
AIRFLOW_CONN_MYSQL_CONN=mysql://root:root@mysql:3306/mydb


```

#### 2. CLI

```bash
airflow connections add 'postgres_conn' \
  --conn-type postgres \
  --conn-host localhost \
  --conn-login myuser \
  --conn-password mypass \
  --conn-schema mydb \
  --conn-port 5432
```

#### 3. Airflow UI

```
Admin → Connections → + Add
Fill in: Conn Id, Conn Type, Host, Schema, Login, Password, Port
```

#### 4. Python Code

```python
from airflow.models import Connection
from airflow import settings

conn = Connection(
    conn_id='postgres_conn',
    conn_type='postgres',
    host='localhost',
    login='myuser',
    password='mypass',
    schema='mydb',
    port=5432
)
session = settings.Session()
session.add(conn)
session.commit()
```

---
---
---

## Database Operators

---
### PostgresOperator

> Run SQL queries or .sql script on PostgreSQL databases.

#### Key Parameters

- `task_id='run_sql'`: Unique task name
- `postgres_conn_id='postgres_default'`: Connection ID from Airflow
- `sql='SELECT * FROM table'`: SQL query or file path
- `parameters={}`: SQL parameters (safe injection)
- `autocommit=False`: Transaction control
- `database=None`: Optional database name
- `do_xcom_push=True`:

📌 **Use case:** Run SQL queries in PostgreSQL (ETL / DB jobs)

```
AIRFLOW_CONN_POSTGRES_CONN=postgresql://myuser:123@my-postgres:5432/mydb
```

```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# ===================== Default Arguments =====================
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 20),
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='postgres_operator_dag',
	catchup=False,
    schedule_interval=None,
    default_args=default_args
    
) as postgres_operator_dag:

    # ===================== Create Table =====================
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id   VARCHAR(50) PRIMARY KEY,
                customer_name VARCHAR NOT NULL,
                address       VARCHAR NOT NULL,
                birth_date    DATE NOT NULL
            );
        """
    )

    # ===================== Insert Data =====================
    insert_values = PostgresOperator(
        task_id='insert_values',
        postgres_conn_id='postgres_conn',
        sql='./scripts/insert.sql'
    )

    # ===================== Select Data =====================
    select_values = PostgresOperator(
        task_id='select_values',
        postgres_conn_id='postgres_conn',
        sql="""
            SELECT *
            FROM customers
            WHERE birth_date BETWEEN %(start_date)s AND %(end_date)s;
        """,
        parameters={
	        'start_date': '1990-01-01',
	        'end_date': '2000-12-31'
	    }
    )

    # ===================== Task Dependencies =====================
    create_table >> insert_values >> select_values
```


---

### MySqlOperator

#### Key Parameters

- `task_id='run_sql'`: Unique task name
- `mysql_conn_id='mysql_default'`: MySQL connection ID in Airflow
- `sql='SELECT * FROM table'`: SQL query or path to SQL file
- `parameters={}`: Query parameters (safe injection)
- `autocommit=False`: Transaction control
- `database=None`: Optional database name

📌 **Use case:** Run SQL queries specifically on MySQL databases (ETL / DB operations)

```python
from airflow.providers.mysql.operators.mysql import MySqlOperator

run_query = MySqlOperator(
    task_id='run_sql',
    mysql_conn_id='mysql_conn',
    sql='SELECT * FROM users'
)
```


---
### SQLExecuteQueryOperator (Generic)

> Works with any database — MySQL, Postgres, SQLite, etc.
#### Key Parameters

- `task_id='run_sql'`: Unique task name
- `sql='SELECT * FROM table'`: SQL query or path to SQL file
- `conn_id='default_conn'`: Database connection ID
- `parameters={}`: Query parameters (safe injection)
- `autocommit=False`: Transaction control
- `database=None`: Optional database name
- 
📌 **Use case:** Run SQL queries on generic databases (MySQL, Postgres, etc.)

```python
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

run_query = SQLExecuteQueryOperator(
    task_id='run_sql',
    conn_id='my_db',
    sql='SELECT * FROM users'
)
```

---

### ClickHouse Operator

#### Key Parameters

- `task_id='run_query'`: Unique task name
- `sql='SELECT * FROM table'`: SQL query أو file path
- `clickhouse_conn_id='clickhouse_conn'`: Airflow connection ID
- `parameters={}`: Query parameters (Jinja templating supported)
- `database='crypto_exchange'`: Optional database


📌 **Use case:** Run SQL queries directly on ClickHouse using Airflow (clean & production-ready)

```python
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

create_table_task = ClickHouseOperator(
    task_id="create_clickhouse_table",
    clickhouse_conn_id="clickhouse_conn",
    sql='SELECT * FROM users'
)
```

```bash

docker compose exec -it airflow-webserver bash

pip install airflow-clickhouse-plugin

```

---
---
---
