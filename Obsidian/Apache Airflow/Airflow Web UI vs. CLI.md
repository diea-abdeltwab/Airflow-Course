

---

##  Web interface.

> Access via browser: `http://localhost:8080`

---

## Essential CLI Commands

### Docker Access

```bash
# Enter the Airflow container
docker compose exec -it airflow-webserver bash
```

---
###  System Diagnosis

```bash
# Show environment, DB, and executor info
airflow info

# Show help menu
airflow -h

# Check DAGs folder location
cat airflow.cfg | grep "dags_folder = "

```

#### Executor

```bash

cat airflow.cfg | grep "executor = "

airflow info | grep executor

```

---

###  DAG Management

```bash
# Check Python syntax errors in DAG file
python3 my_dag.py

# List all loaded DAGs
airflow dags list

# List DAGs that failed to load (syntax/import errors)
airflow dags list-import-errors
```

---

###  Testing & Execution

```bash
# Test a specific task WITHOUT saving to DB
airflow tasks test <dag_id> <task_id> <date>

# Example:
airflow tasks test crypto_pipeline fetch_prices 2026-04-20


# Manually trigger a full DAG run
airflow dags trigger -e <date> <dag_id>

```

---


