

---

### Context and Jinja Templates

>  **Context** and **Jinja Templates** are used to handle dynamic data, but at different stages of execution , **Dynamic Runtime Data**

#### Context (Runtime Data)
- `**context`: Automatically passed runtime dictionary
- `context['key']`: Access values inside context
- Evaluated **during task execution (runtime)**
- Used in `Python` functions (`**context`) to access runtime data.
- `ds` → Execution date (YYYY-MM-DD)
- `ds_nodash` → Execution date without dashes (YYYYMMDD)
- `run_id` → Unique DAG run ID
- `dag` → DAG object
- `task_instance` → Current task instance

```python
{
    'ds': '2026-05-08',                    # Execution date as a string (Dynamic)
    'ds_nodash': '20260508',               # Date string without dashes
    'dag': <DAG: my_pipeline>,             # The current DAG object
    'task': <Task: my_task>,               # The current Task object
    'run_id': 'manual__2026-05-08...',     # Unique ID for this specific run 
    'params': {},                          # User-defined runtime parameters
    'logical_date': datetime(...),         # Precise timestamp of the run
    'data_interval_start': datetime(...),  # Start of the data window
    'data_interval_end': datetime(...),    # End of the data window
    'task_instance': <TaskInstance: ...>,  # Metadata about this task attempt
    # ... and many other metadata keys
}
```
#### Jinja Templates (Pre-execution Rendering)
- Uses `{{ }}` syntax
- Evaluated **before execution (rendering stage)**
- Used in `bash_command`, `sql`, `op_kwargs`, etc.
- `{{ ds }}` → Execution date
- `{{ ds_nodash }}` → Date without dashes
- `{{ run_id }}` → DAG run ID
- `{{ dag.dag_id }}` → DAG name
- `{{ task.task_id }}` → Task name

```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime


def print_jinja_report(dag_id, task_id, run_date, run_id):
    print("====== Pipeline Report ======")
    print(f" DAG ID   : {dag_id}")
    print(f" Task ID  : {task_id}")
    print(f" Run Date : {run_date}")
    print(f" Run ID   : {run_id}")
    print("  Status   : Success")
    print("=============================")

# ===================== Report Function =====================
def print_context_report(**context):
    print("====== Pipeline Report ======")
    print(f" DAG ID   : {context['dag'].dag_id}")
    print(f" Task ID  : {context['task_instance'].task_id}")
    print(f" Run Date : {context['ds']}")
    print(f" Run ID   : {context['run_id']}")
    print("  Status   : Success")
    print("=============================")

# ===================== DAG Definition =====================
with DAG(
    dag_id='context_report_dag',
    start_date=datetime(2026, 4, 20),
    schedule_interval=None,
    catchup=False
) as dag:

    # ===================== Generate Context Report Task =====================
    generate_context_report = PythonOperator(
        task_id='generate_context_report',
        python_callable=print_context_report
    )

    # ===================== Generate Context jinja Task =====================
    generate_python_jinja_report = PythonOperator(
        task_id='generate_python_jinja_report',
        python_callable=print_jinja_report,
        op_kwargs={
            'dag_id': '{{ dag.dag_id }}',
            'task_id': '{{ task.task_id }}',
            'run_date': '{{ ds }}',
            'run_id': '{{ run_id }}'
        }
    )

    # ===================== Generate Jinja Report (Bash) =====================
    generate_bash_jinja_report = BashOperator(
        task_id='generate_bash_jinja_report',
        bash_command="""
        echo "====== Pipeline Report ======"
        echo " DAG ID   : {{ dag.dag_id }}"
        echo " Task ID  : {{ task.task_id }}"
        echo " Run Date : {{ ds }}"
        echo " Run ID   : {{ run_id }}"
        echo " Status   : Success"
        echo "============================="
        """
    )

    generate_context_report >> generate_python_jinja_report >> generate_bash_jinja_report
```

---
---
---

### Variables

> **Variables** are global key-value pairs stored in Airflow's Meta DB.
> Use them for config values shared across multiple DAGs.

#### Setting Variables

**UI:**
```
Admin → Variables → + Add
```

**CLI:**
```bash
docker compose exec airflow-webserver bash

airflow variables set start_date 1990-01-01
airflow variables set end_date   2000-12-31
```

#### Reading Variables

```python
from airflow.models import Variable

# ── In Python ─────────────────────────────────────────────────
start = Variable.get("start_date")
end   = Variable.get("end_date")

```

```sql
-- In Jinja (inside SQL or bash_command )

{{ var.value.start_date }}
{{ var.value.end_date }}

```

```python
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
    dag_id='postgres_operator_dag',
	catchup=False,
    schedule_interval=None,
    default_args=default_args
    
) as postgres_operator_dag:
	
	# ===================== Airflow Variables =====================
    start = Variable.get("start_date") 
    end = Variable.get("end_date")

    # ===================== Create Table =====================
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_conn',
        sql="""
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
        sql='./scripts/insert.sql'
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
```


> **Airflow Variables: The Two Ways**

- **1. Direct Code (`Variable.get`):**
    
    - **How:** `start = Variable.get("start_date")` (written in the middle of the script).
        
    - **When:** Runs every few seconds (**Parsing Time**).
        
    - **Danger:** If the variable is missing, **The DAG disappears** from the UI.
        
- **2. Jinja Template (`{{ var.value }}`):**
    
    - **How:** `'{{ var.value.start_date }}'` (written inside an Operator).
        
    - **When:** Runs only when the task starts (**Runtime**).
        
    - **Safety:** If the variable is missing, **Only the Task fails**, but the DAG stays visible.
    
---
---
---

### Params

> Params are runtime configuration inputs that can be defined globally at the DAG level or overridden per task, enabling flexible and **dynamic** task behavior.

#### Key Parameters

- `params={}`: Pass values to task
- `{{ params.key }}`: Access inside template

```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

# ===================== Python Function =====================
def python_task(**context):
    print("====== Python Task ======")
    print(f"User Name : {context['params']['user_name']}")
    print(f"File Name : {context['params']['file_name']}")
    print(f"Separator  : {context['params']['separator']}")
    print("=========================")

# ===================== DAG =====================
with DAG(
    dag_id='params_mixed_example',
    start_date=datetime(2026, 4, 20),
    schedule_interval=None,
    catchup=False,
    params={
        "user_name": "Diea",
        "file_name": "sales.csv"
    }
) as dag:

    task_python = PythonOperator(
        task_id='python_task',
        python_callable=python_task,
        params={"separator": ","}
    )

    task_bash = BashOperator(
        task_id='bash_task',
        bash_command="""
        echo "====== Bash Task ======"
        echo "User Name: {{ params.user_name }}"
        echo "File Name: {{ params.file_name }}"
        echo "Separator : {{ params.separator }}"
        echo "======================="
        """,
        params={"separator": ";"}
    )

    task_python >> task_bash
```

---
### Jinja Templates

> **Jinja2** is the templating engine Airflow uses to inject runtime values into commands and SQL.

#### Built-in Macros (Most Useful)

| Template                    | Value               | Example               |
| --------------------------- | ------------------- | --------------------- |
| `{{ ds }}`                  | Execution date      | `2024-01-20`          |
| `{{ ds_nodash }}`           | Date without dashes | `20240120`            |
| `{{ ts }}`                  | Full timestamp      | `2024-01-20T00:00:00` |
| `{{ run_id }}`              | DAG run ID          | `manual__2024-01-20`  |
| `{{ dag.dag_id }}`          | DAG name            | `sales_pipeline`      |
| `{{ task.task_id }}`        | Task name           | `extract_task`        |
| `{{ var.value.X }}`         | Variable value      | your variable         |
| `{{ params.X }}`            | Task param          | your param            |
| `{{ macros.uuid.uuid4() }}` | Random UUID         | `abc123...`           |

> **Macros** are built-in helper objects available inside Jinja templates.


---
---
---
