
---
## DAG

---

```python
from airflow import DAG
```

---

### DAG Definition Methods

#### 1. Manual Assignment

```python
# ===================== Import Libraries =====================
from airflow import DAG  
from airflow.operators.bash import BashOperator  
from datetime import datetime  
  
# ===================== Create DAG Object =====================
simple_dag = DAG(  
	dag_id='manual_dag',  
	start_date=datetime(2026, 4, 20)  
)  
  
# ===================== Define Task =====================
print_message = BashOperator(  
	task_id='print_message',  
	bash_command='echo "Hello World!"',  
	dag=simple_dag  
)
```
- `dag = DAG(...)`: Creates a DAG object.
- Tasks must include `dag=dag` manually.
- More error-prone and less clean.

#### 2. Context Manager (`with`)

```python
# ===================== Import Libraries =====================
from airflow import DAG  
from airflow.operators.bash import BashOperator  
from datetime import datetime  
  
# ===================== DAG Definition (Context Manager) =====================
with DAG(  
	dag_id='context_dag',  
	start_date=datetime(2026, 4, 20)  
) as dag:  
  
	# ===================== Define Task =====================
	print_message_task = BashOperator(  
		task_id='print_message',  
		bash_command='echo "Hello World!"'  
	)
	
	
```
- `with DAG(...) as dag`: Creates and manages the DAG.
- Tasks are linked automatically inside the block.
- Cleaner and easier to maintain.
- code organization + automatic DAG assignment

#### 3. Decorator (`@dag`)

```python 
# ===================== Import Libraries =====================
from airflow.decorators import dag, task  
from datetime import datetime  
  
# ===================== DAG Decorator =====================
@dag(  
	dag_id='decorator_dag',  
	start_date=datetime(2026, 4, 20),  
)  
def my_dag():  
  
	# ===================== Task Definition =====================
	@task  
	def print_message():  
		print("Hello World!")  
  
	# ===================== Task Execution =====================
	print_message()  
  
# ===================== Instantiate DAG =====================
dag = my_dag()

```
- `@dag`: Defines the DAG as a Python function
- `@task`: Defines tasks as Python functions
- Automatically handles dependencies and context
- Cleaner, more Pythonic, and less boilerplate


---

### Airflow DAG Configuration Parameters

#### Default Arguments

- `'owner': 'Engineering'`: Defines the DAG owner.
- `'start_date': datetime(2023, 11, 1)`: Start date of the DAG.
- `'retries': 3`: Number of retry attempts.
- `'retry_delay': timedelta(minutes=20)`: Time between retries.
- `'email': ['admin@example.com']`: Email address for notifications.
- `'email_on_failure': False`: Disables failure emails.
- `'email_on_retry': False`:

```python
# ===================== Default Arguments =====================
default_args = {
    'owner': 'data_team',
    'start_date': datetime(2024, 1, 1),
    'end_date'=datetime(2026, 6, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'email': ['alerts@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}
```

#### DAG Configuration 

- `dag_id=update_dataflows'`: DAG unique ID.
- `description='Sales Pipeline'`: Short DAG description.
- `tags=["sales", "daily"]`: Labels for filtering.
- `catchup=False`: Skips past runs.
- `schedule_interval='30 12 * * 3'`: Runs every Wednesday at 12:30.
- `dagrun_timeout=timedelta(minutes=45)`: Max run time limit.

```python
# ===================== DAG Definition =====================
with DAG(
    dag_id='sales_pipeline',           
    description='Daily sales ETL',     
    tags=['sales', 'daily'],  
    catchup=False,                         
    schedule_interval='@daily',       
    dagrun_timeout=timedelta(minutes=45),          
    default_args=default_args
) as simple_dag:
    pass
```

---

### Retries and Manual Trigger

```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# ===================== Default Arguments =====================
dag_default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 20),
    'retries': 3,
    'retry_delay': 5
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='manual_dag',
    description='My First DAG',
    tags=['print_message'],
    catchup=False,
    schedule_interval=None,
    default_args=dag_default_args
) as simple_dag:

    # ===================== Define Task =====================
    print_message_task = BashOperator(
        task_id='print_message',
        bash_command='echo "Hello World!"' #'sleep 3; exit 1'
    )
    
```


---

## Schedule Intervals

### Presets

- Schedule the task at `start_date` + `schedule_interval`

| Preset       | When                  |
| ------------ | --------------------- |
| `None`       | Never — manual only   |
| `'@once'`    | Run exactly once      |
| `'@hourly'`  | Every hour            |
| `'@daily'`   | Every day at midnight |
| `'@weekly'`  | Every Sunday          |
| `'@monthly'` | 1st of every month    |
| `'@yearly'`  | January 1st           |

### Cron Syntax

```
* * * * *
│ │ │ │ │
│ │ │ │ └── Day of week (0–7)
│ │ │ └──── Month (1–12)
│ │ └────── Day of month (1–31)
│ └──────── Hour (0–23)
└────────── Minute (0–59)


'30 12 * * *'    → every day at 12:30
'0  9 * * 1'     → every Monday at 09:00
'*/15 * * * *'   → every 15 minutes
'0 0 1,15 * *'   → every 1st and 15th of the month at 00:00
'0 0 * * 5-6'    → every Friday and Saturday at 00:00

```

---

####  Presets and Cron Schedule

```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# ===================== Default Arguments =====================
dag_default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 5, 7),
}

# ===================== DAG Definition =====================
with DAG(
    dag_id='trigger_dag',
    description='My First DAG',
    tags=['print_message'],
    catchup=False,
    schedule_interval='@hourly',             #'30 12 * * 3'
    default_args=dag_default_args
) as simple_dag:

    # ===================== Define Task =====================
    print_message_task = BashOperator(
        task_id='print_message',
        bash_command='echo "Hello World!"'
    )
```

---
---
---
