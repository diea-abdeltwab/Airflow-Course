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