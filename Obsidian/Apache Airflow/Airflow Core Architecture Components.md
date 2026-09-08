
---

### Docker

```bash

docker ps

```

---

###  Airflow Core Architecture Components

- **DAG Processor:** Parses and loads DAGs.
	
- **Scheduler:** Decides **when** tasks run.
	
- **Executor:** Defines **how** tasks are executed:
    
    - `SequentialExecutor`: One task at a time (Dev only).
        
    - `LocalExecutor`: Parallel tasks on one machine.
        
    - `KubernetesExecutor`: Each task runs in its own Pod .
		
	- `CeleryExecutor`: Distributed across **multiple** machines .
	
- **Redis:** Queues and distributes tasks.
	
- **Worker:** Executes the tasks.
    
- **Triggerer:** Runs async tasks without blocking workers.
	
- **Meta Database:** Stores all workflow states and configs.
	
- **Web Server:** Airflow UI for monitoring and troubleshooting.

---

![[Pasted image 20260428090452.png]]

---
---
---

![[Pasted image 20260428090420.png]]

---
---
---
