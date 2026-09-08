
----

> An **Operator** defines *what* a task does. Each operator is a template 

---

```python

# ===================== Operators (Core) =====================

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator


# ===================== Providers (External Systems) =====================

from airflow.providers.postgres.operators.postgres import PostgresOperator


# ===================== Sensors =====================

from airflow.sensors.filesystem import FileSensor
from airflow.sensors.sql import SqlSensor

```

---
### Bash Operator

> Run shell commands or `.sh` scripts.
#### Key Parameters

- `task_id='run_script'`: Unique task name
- `bash_command='script.sh'`: Shell command or script to execute
- `env={}`: Environment variables (optional)

📌 **Use case:** Run shell scripts / Linux commands


```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# ===================== Default Arguments =====================
dag_default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 20),
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='bash_operator_dag',
    catchup=False,
    schedule_interval=None,
    default_args=dag_default_args,
) as bash_operator_dag:

    # ===================== Task 1: Print Message =====================
    print_message_task = BashOperator(
        task_id='print_message',
        bash_command='echo "Hello from (print_message)"',
    )

    # ===================== Task 2: Run Shell Script =====================
    run_script_task = BashOperator(
        task_id='run_script',
        bash_command='./scripts/print_message_script.sh',
    )

    # ===================== Task 3: Run Script with env =====================
    run_script_with_env_task = BashOperator(
        task_id='run_script_with_env',
        bash_command='bash /opt/airflow/dags/scripts/print_message_with_env_script.sh ',
        env={
            'MY_NAME': 'Airflow',
            'TASK_NAME': 'run_script_with_env_task',
        },
    )

    # ===================== Define Task Dependencies =====================
    print_message_task >> run_script_task >> run_script_with_env_task 
```


>**Run shell commands or .sh scripts:**

- **Direct Command:** Use **Absolute Path** with the `bash` command.
    
    - **Essential:** Ensure the string ends with a **space** (e.g., `'bash /path/to/script.sh '`).
        
    - **Why?** This prevents Airflow from treating the path as a Jinja template, executing it immediately as a direct shell command.
        
- **Template File:** Use **Relative Path** (from the DAGs folder) **without** the `bash` command.
    
    - **Essential:** The string **must end** with `.sh` (with no trailing space).
        
    - **Why?** This tells Airflow to open the file, render any Jinja templates inside (like `{{ ds }}`), and then execute the content.

---
### Python Operator

> Execute a Python function directly
#### Key Parameters

- `task_id='python_task'`: Unique task name
- `python_callable=my_function`: Function to execute
- `op_args=[]`: Positional arguments
- `op_kwargs={}`: Keyword arguments

📌 **Use case:** Custom Python logic / ETL / data processing

```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime


# ===================== Python Functions =====================

def print_hello():
    print("Hello from PythonOperator!")


def print_hello_with_args (my_name, task_name):
    print(f"Hello {my_name}, from {task_name}")

# ===================== Default Arguments =====================
dag_default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 20),
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='python_dag',
    catchup=False,
    schedule_interval=None,
    default_args=dag_default_args
) as python_operator_dag:

    # ===================== Task 1: Simple Python Function =====================
    print_message_task = PythonOperator(
        task_id='print_message',
        python_callable=print_hello
    )

    #=================== Task 2: Function with Positional Args ==================
    print_message_with_args_task = PythonOperator(
        task_id='print_message_with_env',
        python_callable=print_hello_with_args,
        op_args=['Airflow', 'print_message_with_env']
    )

    #===================== Task 3: Function with Keyword Args ===================
    print_message_with_kwargs_task = PythonOperator(
        task_id='print_message_with_env_2',
        python_callable=print_hello_with_args,
        op_kwargs={
            'my_name': 'Airflow',
            'task_name': 'print_message_with_env_2'
        }
    )

    #===================== Task 4: Run External Python Script ===================
    run_python_script_task = BashOperator(
        task_id='print_message_bash',
        bash_command='python /opt/airflow/dags/scripts/print_message_py_script.py'
    )

    # ===================== Define Task Dependencies =====================
    print_message_task >> print_message_with_args_task >> print_message_with_kwargs_task >> run_python_script_task
```

---

### Empty Operator

> A placeholder task — does nothing, just marks a point in the DAG.
#### Key Parameters

- `task_id='empty_task'`: Unique task name

📌 **Use case:** Modern replacement of DummyOperator

```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# ===================== Default Arguments =====================
dag_default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 20),
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='empty_operator_dag',
    catchup=False,
    schedule_interval=None,
    default_args=dag_default_args
) as empty_operator_dag:

    # ===================== Start Task =====================
    start_task = EmptyOperator(
        task_id='start'
    )

    # ===================== Middle Task =====================
    middle_task = EmptyOperator(
        task_id='task'
    )

    # ===================== End Task =====================
    end_task = EmptyOperator(
        task_id='end'
    )

    # ===================== Task Dependencies =====================
    start_task >> middle_task >> end_task
```

---
### Email Operator

> Send emails for notifications, reports, or alerts.
#### Key Parameters

- `task_id='send_email'`: Unique task name
- `to='user@example.com'`: Receiver email
- `subject='Report'`: Email subject
- `html_content='<h1>Report</h1>'`: Email body (HTML supported)
- `files=[]`: Attachments


📌 **Use case:** Notifications / reports / alerts

```
AIRFLOW__SMTP__SMTP_HOST="smtp.sendgrid.net"
AIRFLOW__SMTP__SMTP_PORT="587"
AIRFLOW__SMTP__SMTP_USER="apikey"
AIRFLOW__SMTP__SMTP_PASSWORD=""
AIRFLOW__SMTP__SMTP_MAIL_FROM=""
```

```python
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
    dag_id='email_operator_dag',
    catchup=False,
    schedule_interval=None,
    default_args=dag_default_args
) as email_operator_dag:

    # ===================== Email Task: Send Report =====================
    send_report = EmailOperator(
        task_id='send_report',
        to='tempm7513@gmail.com',
        subject='Report',
        html_content='<h1>This is the report.</h1> <p>This is the subject.</p> '
    )
```

---
---
---
