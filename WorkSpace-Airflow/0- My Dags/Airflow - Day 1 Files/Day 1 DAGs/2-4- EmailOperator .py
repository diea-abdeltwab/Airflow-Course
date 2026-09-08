# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.email import EmailOperator
from datetime import datetime

# ===================== Default Arguments =====================
dag_default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 20),
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='2_4_email_operator_dag',
    catchup=False,
    schedule_interval=None,
    default_args=dag_default_args
) as email_operator_dag:

    # ===================== Email Task: Send Report =====================
    send_report = EmailOperator(
        task_id='send_report',
        to='tempm7513@gmail.com',
        subject='Report',
        html_content='<h1>This is the report.</h1> <p>This is the subject.</p>'
    )