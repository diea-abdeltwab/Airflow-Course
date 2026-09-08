
---

> Airflow connects to Big Data tools through **Providers**.
> Each provider installs the needed Operators and Hooks.

---
---
---

## Spark

---
#### Connection Setup

```
AIRFLOW_CONN_SPARK_DEFAULT='spark://spark-master:7077'
```

---
### SparkSubmitOperator

> Submit a Spark job from Airflow.

#### Key Parameters

- `task_id`: Unique task name
- `application`: Path to the `.py` or `.jar` file
- `conn_id='spark_default'`: Spark connection ID
- `application_args=[]`: Arguments passed to the script
- `conf={}`: Spark config overrides
- `executor_memory`: e.g. `'2g'`
- `total_executor_cores`: Number of cores

```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# ===================== DAG Definition =====================
with DAG(
    dag_id='spark_dag',
    start_date=datetime(2026, 4, 20),
    schedule_interval=None,
    catchup=False,
) as dag:

    # ===================== Spark Submit Job =====================
    run_spark_job = SparkSubmitOperator(
        task_id='run_spark_job',
        application='/opt/airflow/dags/scripts/process_sales.py',
        conn_id='spark_default',
        application_args=['--date', '{{ ds }}'],
        conf={ 
	        "spark.executor.memory": "2g",
	         "spark.cores.max": "2" 
	    }
    )
```


---
---
---

## Hadoop

---
#### Connection Setup

```
AIRFLOW_CONN_HDFS_DEFAULT='hdfs://namenode:9870'
```

---
## HDFS Sensor

> Interact with Hadoop HDFS — check files, move data.

#### Key Parameters

- `task_id`: A unique name for the task in Airflow.
- `filepath`: The file path in HDFS. It can use templates like `{{ ds_nodash }}`.
- `hdfs_conn_id='hdfs_default'`: The Airflow connection ID for HDFS.
- `poke_interval`: Time (in seconds) between each check (e.g., every 60 seconds).
- `timeout`: Maximum waiting time before the task fails (in seconds).
- `mode='reschedule'`: Frees the worker slot while waiting (better for performance).

```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.providers.apache.hdfs.sensors.hdfs import HdfsSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

# ===================== DAG Definition =====================
with DAG(
    dag_id='hdfs_spark_pipeline',
    start_date=datetime(2026, 4, 20),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # ===================== Sensor: Wait for HDFS File =====================
    wait_for_file = HdfsSensor(
        task_id='wait_for_hdfs_file',
        filepath='/data/input/sales_{{ ds_nodash }}.csv',
        hdfs_conn_id='hdfs_default',
        poke_interval=60,
        timeout=3600,
        mode='reschedule',
    )

    # ===================== Spark Processing Task =====================
    process = SparkSubmitOperator(
        task_id='process_with_spark',
        application='/opt/airflow/dags/scripts/process_hdfs.py',
        conn_id='spark_default',
        application_args=['--date', '{{ ds }}'],
    )

    # ===================== Task Dependencies =====================
    wait_for_file >> process
```


---
---
---

## Kafka
#### Connection Setup

```
AIRFLOW_CONN_KAFKA_DEFAULT='kafka://?bootstrap.servers=kafka:9092'
```

---
### ProduceToTopicOperator 

> Publish messages to a Kafka topic from Airflow.

#### Key Parameters

- `task_id`: Unique task name
- `topic`: Kafka topic name
- `producer_function`: Function that yields messages
- `kafka_config_id='kafka_default'`: Kafka connection ID 

```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from datetime import datetime


# ===================== Producer Function =====================
def produce_messages(ds):
    yield ('key_1', '{"event": "sale", "amount": 500}')
    yield ('key_2', '{"event": "sale", "amount": 750}')


# ===================== DAG Definition =====================
with DAG(
    dag_id='kafka_produce',
    start_date=datetime(2026, 4, 20),
    schedule_interval=None,
    catchup=False,
) as dag:

    # ===================== Kafka Producer Task =====================
    publish = ProduceToTopicOperator(
        task_id='publish_to_kafka',
        topic='sales_events',
        producer_function=produce_messages,
        kafka_config_id='kafka_default',
    )
```

---

### ConsumeFromTopicOperator 

> Read messages from a Kafka topic.

#### Key Parameters

- `task_id`: Unique task name
- `topics`: List of topic names to consume from
- `apply_function`: Function called for each message
- `kafka_config_id`: Kafka connection ID
- `max_messages`: Stop after N messages

```python
# ===================== Import Libraries =====================
from airflow import DAG
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from datetime import datetime


# ===================== Message Processing Function =====================
def process_message(message):
    print(f"Key: {message.key()} | Value: {message.value()}")


# ===================== DAG Definition =====================
with DAG(
    dag_id='kafka_consume',
    start_date=datetime(2026, 4, 20),
    schedule_interval=None,
    catchup=False,
) as dag:

    # ===================== Kafka Consumer Task =====================
    consume = ConsumeFromTopicOperator(
        task_id='consume_from_kafka',
        topics=['sales_events'],
        apply_function=process_message,
        kafka_config_id='kafka_default',
        max_messages=100,
    )
```

---
---
---

## Summary

| Tool | Operator / Sensor | Provider Package |
|------|-------------------|-----------------|
| **Spark** | `SparkSubmitOperator` | `apache-airflow-providers-apache-spark` |
| **Kafka** | `ProduceToTopicOperator` | `apache-airflow-providers-apache-kafka` |
| **Kafka** | `ConsumeFromTopicOperator` | `apache-airflow-providers-apache-kafka` |
| **HDFS** | `HdfsSensor` | `apache-airflow-providers-apache-hdfs` |
| **BigQuery** | `BigQueryInsertJobOperator` | `apache-airflow-providers-google` |
| **EMR (AWS)** | `EmrCreateJobFlowOperator` | `apache-airflow-providers-amazon` |

---
---
---