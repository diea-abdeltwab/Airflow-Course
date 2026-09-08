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