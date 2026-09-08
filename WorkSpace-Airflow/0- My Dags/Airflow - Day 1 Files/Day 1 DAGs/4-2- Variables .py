# ===================== Import Libraries =====================
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from airflow.models import Variable

# ===================== Default Arguments =====================
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 20),
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='4_2_postgres_operator_dag',
	catchup=False,
    schedule_interval=None,
    default_args=default_args
    
) as dag:
	
	# ===================== Airflow Variables =====================
    start = Variable.get('start_date') 
    end = Variable.get('end_date')

    # ===================== Create Table =====================
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_conn',
        sql="""
            Drop table if exists customers;
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
        sql='./scripts/insert.sql',
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
	        'start_date': start, # '{{ var.value.start_date }}'
	        'end_date':   end    # '{{ var.value.end_date }}'
	    }
    )

    # ===================== Task Dependencies =====================
    create_table >> insert_values >> select_values