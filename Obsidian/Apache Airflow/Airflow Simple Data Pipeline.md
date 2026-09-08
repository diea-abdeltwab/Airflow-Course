
---
---
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
    dag_id='4_4_data_cleaning_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={
        "base_path": "/opt/airflow/dags/data",
        "user_role": "admin",
    }
) as dag:

    # ===================== Task 1: Extract & Clean =====================
    extract_and_clean_batch = BashOperator(
        task_id='extract_and_clean_batch',
        bash_command="""
                echo "Processing {{ params.input_file }}..."
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
    extract_and_clean_batch >> generate_report
```

---

```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable

from scripts.postgres_load_pipeline_utils import (
    csv_to_sql,
    CREATE_SALES_SQL
)

# ===================== Default Arguments =====================
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 20),
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='4_5_csv_to_postgres_pipeline',
    catchup=False,
    schedule_interval=None,
    default_args=default_args
) as dag:

    # ===================== Create Table =====================
    create_sales_table = PostgresOperator(
        task_id='create_sales_table',
        postgres_conn_id='postgres_conn',
        sql=CREATE_SALES_SQL,
    )

    # ===================== Transform (CSV -> SQL file) =====================
    transform_sales = PythonOperator(
        task_id='transform_sales',
        python_callable=csv_to_sql,
        op_kwargs={'table_name': 'sales', 'file_name': 'sales'},
    )

    # ===================== Load SQL =====================
    load_sales = PostgresOperator(
        task_id='load_sales',
        postgres_conn_id='postgres_conn',
        sql='./data/sql/insert_sales_cleaned.sql',
    )

    # ===================== Select Data =====================
    select_sales = PostgresOperator(
        task_id='select_sales',
        postgres_conn_id='postgres_conn',
        sql="""
            SELECT *
            FROM sales
            WHERE date BETWEEN '{{ var.value.start_date }}'
            AND '{{ var.value.end_date }}';
        """
    )

    # ===================== Task Dependencies =====================
    create_sales_table >> transform_sales >> load_sales >> select_sales
```

---
---
---

### How to Install a Package in Airflow ?

```bash
docker compose exec airflow-webserver bash
pip install pandas

#--------------------------------------------------------------------

_PIP_ADDITIONAL_REQUIREMENTS: "pandas"
```


---
---
---
