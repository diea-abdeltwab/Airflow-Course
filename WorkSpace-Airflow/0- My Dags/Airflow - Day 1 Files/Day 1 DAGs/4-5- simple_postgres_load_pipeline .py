
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
        WHERE date BETWEEN '{{ var.value.start_date }}' AND '{{ var.value.end_date }}' ;
    """
    )
    # ===================== Task Dependencies =====================
    create_sales_table >> transform_sales >> load_sales >> select_sales