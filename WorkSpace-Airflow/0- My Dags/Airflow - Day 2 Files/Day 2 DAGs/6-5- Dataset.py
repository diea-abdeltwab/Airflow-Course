# ===================== Import Libraries =====================
from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from datetime import datetime

# ===================== Dataset Path =====================
FILE_PATH = "/opt/airflow/dags/data/raw/raw_sales.csv"
MY_DATA_FILE = Dataset(f"file://{FILE_PATH}")

# ========================================================
# ===================== PRODUCER DAG =====================
# ========================================================
with DAG(
    dag_id='6_5_1_producer_setup_pipeline',
    start_date=datetime(2026, 5, 1),
    schedule='@daily',
    catchup=False
) as producer_dag:

    # ===================== Setup & Update Dataset =====================
    setup_and_update = BashOperator(
        task_id='setup_and_update_data',
        bash_command=(
            f'echo "id,product,amount,date\n'
            f'1,Laptop,1200,2026-05-01\n'
            f'2,Mouse,25,2026-05-01" > {FILE_PATH}'
        ),
        outlets=[MY_DATA_FILE]
    )

# ========================================================
# ===================== CONSUMER DAG =====================
# ========================================================
with DAG(
    dag_id='6_5_2_consumer_etl_pipeline',
    start_date=datetime(2026, 5, 1),
    schedule=[MY_DATA_FILE],
    catchup=False
) as consumer_dag:

    # ===================== Extract Data =====================
    extract = BashOperator(
        task_id='extract_data',
        bash_command=f'cat {FILE_PATH}'
    )

    # ===================== Transform Data =====================
    transform = BashOperator(
        task_id='transform_data',
        bash_command='echo "Calculating total sales and cleaning data..."'
    )

    # ===================== Load Data =====================
    load = BashOperator(
        task_id='load_data',
        bash_command='echo "Loading data into destination system..."'
    )

    # ===================== Task Dependencies =====================
    extract >> transform >> load