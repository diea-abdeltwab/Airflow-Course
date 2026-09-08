

---

> A **Sensor** is a special operator that **waits** for a condition to be true before allowing the pipeline to continue.

---
### File Sensor

#### Key Parameters

- `task_id='wait_for_file'`: Unique task name
- `filepath='path/to/file'`: File to watch
- `fs_conn_id='fs_default'`: Filesystem connection
- `poke_interval=60`: Check interval (seconds)
- `timeout=3600`: Max wait time
- `mode='poke'`: Or 'reschedule'
- `soft_fail=True`: Skip task on timeout

📌 **Use case:** Wait until a file exists before continuing pipeline

```
AIRFLOW_CONN_FS_DEFAULT=fs:/opt/airflow/
```

```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# ===================== Default Arguments =====================
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 20),
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='file_sensor_dag',
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as file_sensor_dag:

    # ===================== File Sensor Task =====================
    file_sensor = FileSensor(
        task_id='file_sensor',
        filepath='dags/data/raw/sales.csv',
        mode='poke',                        #reschedule
        poke_interval=5,
        timeout=30,
        #soft_fail=True,
    )

    # ===================== Empty Task =====================
    empty_task = EmptyOperator(
        task_id='empty_task',
    )

    # ===================== Task Dependencies =====================
    file_sensor >> empty_task
```

---

```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator 
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta               
 

# ===================== Default Arguments =====================
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 20),
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='data_cleaning_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={
        "base_path": "/opt/airflow/dags/data",
        "user_role": "admin",
    }
) as dag:

	# ===================== Sensor: Wait for Input File =====================
    wait_for_sales_file = FileSensor(
        task_id='wait_for_sales_file',
        filepath='dags/data/raw/sales.csv',
        poke_interval=10,
        timeout=120,
        mode='reschedule',
    )
    # ===================== Task 1: Extract & Clean =====================
    extract_and_clean_batch = BashOperator(
        task_id='extract_and_clean_batch',
        bash_command= """
                echo "Processing {{ params.input_file  }}..."
                python /opt/airflow/dags/scripts/cleandata.py \
                {{ ds_nodash }} \
                {{ params.base_path }}/raw/{{ params.input_file }} \
                {{ params.base_path }}/cleaned
                """,
        params={
            'input_file': 'sales.csv'
        }
    )

    # ===================== Generate Report =====================
    generate_report = BashOperator(
        task_id='generate_report',
        bash_command="""
            echo "Pipeline Run Date: {{ ds_nodash }}" && \
            echo "DAG: {{ dag.dag_id }}" && \
            echo "Output Folder: {{ params.base_path }}/cleaned" && \
            echo "Executed By: {{ params.user_role }}"
        """
    )
    # ===================== Task Dependencies =====================
    wait_for_sales_file >> extract_and_clean_batch >> generate_report 
```

---

### SQL Sensor

#### Key Parameters

- `task_id='wait_for_sql'`: Unique task name
- `conn_id='db_conn'`: Database connection ID
- `sql='SELECT 1 FROM table'`: Query condition
- `poke_interval=60`: Check interval
- `timeout=3600`: Max wait time
- `mode='poke'`: Or 'reschedule'
- `soft_fail=True`: Skip task on timeout

📌 **Use case:** Wait for DB condition to be true before continuing

```
AIRFLOW_CONN_POSTGRES_CONN=postgresql://myuser:123@my-postgres:5432/mydb
```

```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# ===================== Default Arguments =====================
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 20),
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='sql_sensor_dag',
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as sql_sensor_dag:

    # ===================== SQL Sensor: Wait for Data =====================
    sql_sensor = SqlSensor(
        task_id='sql_sensor',
        conn_id='postgres_conn',
        sql="SELECT 1 FROM customers LIMIT 1;", # WHERE created_at = CURRENT_DATE
        mode='reschedule',
        poke_interval=5,
        timeout=30,
        soft_fail=True,
    )

    # ===================== Empty Task =====================
    empty_task = EmptyOperator(
        task_id='empty_task',
    )

    # ===================== Task Dependencies =====================
    sql_sensor >> empty_task
```



```sql
-- ===================== Drop Table If Exists =====================
DROP TABLE IF EXISTS customers;

-- ===================== Create Table =====================
CREATE TABLE IF NOT EXISTS customers (
    customer_id     VARCHAR(50) PRIMARY KEY,
    customer_name   VARCHAR NOT NULL,
    address         VARCHAR NOT NULL,
    birth_date      DATE NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_DATE
);

-- ===================== Insert Initial Batch Data =====================
INSERT INTO customers (customer_id, customer_name, address, birth_date ,created_at) VALUES
('C001', 'Ahmed Ali', 'Cairo, Nasr City', '2001-05-12', CURRENT_DATE - 1),
('C002', 'Sara Mohamed', 'Giza, Dokki', '1999-11-23', CURRENT_DATE - 1),
('C003', 'Omar Hassan', 'Alexandria, Sidi Gaber', '2003-02-17', CURRENT_DATE - 1),
('C004', 'Mona Adel', 'Cairo, Maadi', '2000-07-30', CURRENT_DATE - 1),
('C005', 'Youssef Tarek', 'Giza, 6th of October', '2004-09-05', CURRENT_DATE - 1),
('C006', 'Nour Khaled', 'Mansoura, Downtown', '2002-12-01', CURRENT_DATE - 1),
('C007', 'Hana Samir', 'Cairo, Heliopolis', '1998-03-14', CURRENT_DATE - 1),
('C008', 'Karim Magdy', 'Tanta, Center', '2005-06-21', CURRENT_DATE - 1);

-- ===================== Insert Single Record =====================
INSERT INTO customers (customer_id, customer_name, address, birth_date ,created_at) VALUES
('C009', 'Mohamed Ahmed', 'Cairo, Zamalek', '2000-01-01', DEFAULT);
```

---

