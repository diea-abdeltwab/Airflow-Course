
---

Apache Airflow is an open-source platform used to Create, schedule, and monitor workflows. Workflows are primary written in **Python**, though they can execute components in other languages.

---

![[Pasted image 20260428085054.png]]

---
### 1. The DAG (Directed Acyclic Graph)

> The **container** that defines your workflow — task order, schedule, and config.

- **Directed:** Logical flow of dependencies.
    
- **Acyclic:** No loops or cycles.
    
- **Graph:** Collection of tasks and relationships.
    
- **Usage:**  **Apache Airflow** ( Workflow ) , **Apache Spark** ( Execution Plan ) .
	

---
### 2. Triggers

> Events that **start** a DAG run.

- **Methods:** Time or Manual or External.
	

---
### 3. Operators

> A **template** that defines what a single task does.

- **Action:** `BashOperator`, `PythonOperator`, `PostgresOperator`,`EmailOperator`,
		 `SparkSubmitOperator` , `BranchPythonOperator` .
    
- **Transfer:** `S3ToGCSOperator`, `MySqlToGCSOperator`.
    
- **Custom:** User-defined for specific needs.
	


---
### 4. Sensors

> Operators that **wait** for a condition.

- **Types:** `FileSensor`, `SqlSensor`.
    
- **Function:** Checks for files, records, or API responses.
	

---
### 5. Tasks  & Dependencies

- **Upstream (`>>`):** Task A must finish before B.
    
- **Downstream (`<<`):** Opposite of upstream.
	

```python
# Upstream: A runs before B
task_a >> task_b

# Downstream: same as above, reversed syntax
task_b << task_a

# Parallel: A and B both run before C
[task_a, task_b] >> task_c
```

---

![[Pasted image 20260428085409.png]]

---
