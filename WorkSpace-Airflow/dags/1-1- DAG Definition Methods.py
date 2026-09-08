#=========================================================================
#========================== Manual DAG Definition ========================
#=========================================================================
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# ===================== Create DAG Object =====================
simple_dag = DAG(
    dag_id='1_1_1_manual_dag',
    start_date=datetime(2026, 4, 20)
)

# ===================== Define Task =====================
print_message_task = BashOperator(
    task_id='print_message',
    bash_command='echo "Hello World!"',
    dag=simple_dag
)


#=========================================================================
#====================== Context Manager DAG Definition ===================
#=========================================================================

#from airflow import DAG
#from airflow.operators.bash import BashOperator
#from datetime import datetime

# ================ Create DAG Using Context Manager =====================
with DAG(
    dag_id='1_1_2_context_dag',
    start_date=datetime(2026, 4, 20)
) as dag:

    # ===================== Define Task =====================
    print_message_task = BashOperator(
        task_id='print_message',
        bash_command='echo "Hello World!"'
    )

#=========================================================================
#====================== Decorator-Based DAG Definition ===================
#=========================================================================

from airflow.decorators import dag, task
#from datetime import datetime

# ===================== DAG Decorator =====================
@dag(
    dag_id='1_1_3_decorator_dag',
    start_date=datetime(2026, 4, 20),
)
def my_dag():

    # ===================== Task Decorator =====================
    @task
    def print_message():
        print("Hello World!")

    # ===================== Task Execution =====================
    print_message()

# ===================== Instantiate DAG =====================
dag = my_dag()

#=========================================================================