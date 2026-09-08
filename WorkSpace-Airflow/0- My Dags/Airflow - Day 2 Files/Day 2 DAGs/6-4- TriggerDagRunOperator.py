# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

#======================================================================
# ============================ Setup Pipeline DAG =====================
#======================================================================
with DAG(
    dag_id='6_4_1_setup_pipeline',
    start_date=datetime(2026, 4, 20),
    schedule_interval=None,
    catchup=False
) as dag:

    # ===================== Setup Task =====================
    setup = BashOperator(
        task_id='Setup',
        bash_command='echo "Setting up Pipeline..."'
    )

    # ===================== Trigger ETL DAG =====================
    trigger_etl = TriggerDagRunOperator(
        task_id='Trigger_ETL',
        trigger_dag_id='6_4_2_ETL_pipeline'
    )

    # ===================== Task Dependencies =====================
    setup >> trigger_etl

#======================================================================
# ============================== ETL Pipeline DAG =====================
#======================================================================
with DAG(
    dag_id='6_4_2_ETL_pipeline',
    start_date=datetime(2026, 4, 20),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    # ===================== Extract Task =====================
    extract = BashOperator(
        task_id='Extract',
        bash_command='echo "Extracting data..."'
    )

    # ===================== Transform Task =====================
    transform = BashOperator(
        task_id='Transform',
        bash_command='echo "Transforming data..."'
    )

    # ===================== Load Task =====================
    load = BashOperator(
        task_id='Load',
        bash_command='echo "Loading data..."'
    )

    # ===================== Task Dependencies =====================
    extract >> transform >> load