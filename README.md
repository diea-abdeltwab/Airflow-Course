<div align="center">

[![Airflow Course banner](https://capsule-render.vercel.app/api?type=waving&color=0:111214,50:16324a,100:0F2238&height=220&section=header&text=Airflow-Course&fontSize=52&fontColor=EDEDEC&animation=fadeIn&fontAlignY=38&desc=Two-Day%20Apache%20Airflow%20Bootcamp%20%E2%80%94%20DAGs%2C%20Labs%2C%20Notes%20%26%20Docker%20Setup&descAlignY=58&descSize=17&descColor=8B939B)](https://capsule-render.vercel.app)

[![Typing SVG](https://readme-typing-svg.herokuapp.com?font=Fira+Code&weight=600&size=20&duration=3000&pause=800&color=EDEDEC&background=00000000&center=true&vCenter=true&width=700&lines=25%2B+production-style+DAGs%2C+day+by+day;Sensors%2C+Hooks%2C+XComs%2C+Pools%2C+SLAs%2C+Branching;Docker-based+local+Airflow+cluster;Full+Obsidian+notes+vault+included.)](https://readme-typing-svg.herokuapp.com)

<br/>

[![Stars](https://img.shields.io/github/stars/diea-abdeltwab/Airflow-Course?style=for-the-badge&color=F1C40F&labelColor=111214)](https://github.com/diea-abdeltwab/Airflow-Course/stargazers)
[![Forks](https://img.shields.io/github/forks/diea-abdeltwab/Airflow-Course?style=for-the-badge&color=5DADE2&labelColor=111214)](https://github.com/diea-abdeltwab/Airflow-Course/network/members)
[![Last Commit](https://img.shields.io/github/last-commit/diea-abdeltwab/Airflow-Course?style=for-the-badge&color=58D68D&labelColor=111214)](https://github.com/diea-abdeltwab/Airflow-Course/commits/main)
[![License](https://img.shields.io/badge/License-MIT-7A2E2E?style=for-the-badge&labelColor=111214)](LICENSE)

<br/>

[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.0-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-47A248?style=for-the-badge&logo=mongodb&logoColor=white)](https://www.mongodb.com/)
[![Obsidian](https://img.shields.io/badge/Obsidian-7C3AED?style=for-the-badge&logo=obsidian&logoColor=white)](https://obsidian.md/)

**[About](#-about) · [Features](#-features) · [Quick Start](#-quick-start) · [Project Structure](#%EF%B8%8F-project-structure) · [Course Outline](#%EF%B8%8F-course-outline) · [Roadmap](#-roadmap) · [Author](#-author)**

</div>

---

## 📖 About

**Airflow-Course** is the full local setup used to teach a two-day **Apache Airflow** bootcamp — a ready-to-run Docker Airflow cluster with every DAG built live during the sessions, plus the complete Obsidian notes vault used alongside the slides.

The repo is split into two independent top-level pieces:

<div align="center">

| Folder | What it is |
| :---: | --- |
| 🐳 [`WorkSpace-Airflow/`](WorkSpace-Airflow) | The actual Airflow project — Docker Compose cluster, all course DAGs, sample data & scripts |
| 🗒️ [`Obsidian/`](Obsidian) | The Obsidian vault used to teach the course — concept notes and Excalidraw architecture diagrams |

</div>

Each file inside `WorkSpace-Airflow` is numbered in the order it was taught, so the progression from a one-task `BashOperator` DAG to Sensors, Hooks, XComs, Pools, SLAs, and dynamic branching is easy to follow — and every concept has a matching note in `Obsidian/`.

<div align="right"><a href="#airflow-course">↑ back to top</a></div>

---

## ✨ Features

<table>
<tr>
<td width="50%" valign="top">

### 🐳 Cluster & Setup
- One-command Airflow cluster via official `apache/airflow:2.9.0` `docker-compose.yaml`
- `CeleryExecutor` + Redis + PostgreSQL, ready for local dev
- Multi-source ready: pre-wired connections for PostgreSQL, MongoDB, the local filesystem, and SendGrid SMTP for alerting

</td>
<td width="50%" valign="top">

### 🗓️ Curriculum
- Day-by-day DAG progression, numbered `1-1-` → `11-2-`, mapped to the course outline
- Every core building block: operators, sensors, XComs, hooks, pools, SLAs, branching, dynamic tasks
- Hands-on lab briefs per day to practice each concept yourself

</td>
</tr>
</table>

- 🧩 **Every core Airflow building block covered** — DAG definition styles, retries & schedules, Bash/Python/Empty/Email/Postgres operators, context, Variables, Params, Sensors (`FileSensor`, `SqlSensor`, `MongoSensor`), trigger rules, branching, XComs, Hooks, `SQLAlchemy`, Pools, SLAs & callbacks
- 🗒️ **Obsidian vault included** — 19 linked concept notes plus Excalidraw architecture diagrams; open `Obsidian/` directly as a vault

<div align="right"><a href="#airflow-course">↑ back to top</a></div>

---

## 📦 Requirements

| Tool | Notes |
| --- | --- |
| [Docker](https://www.docker.com/) & [Docker Compose](https://docs.docker.com/compose/) | v2 recommended |
| RAM | 4 GB+ available to Docker (official Airflow quick-start minimum) |
| [Obsidian](https://obsidian.md/) | *Optional* — to browse `Obsidian/` as an interactive vault instead of plain Markdown |

---

## 🚀 Quick Start

```bash
# 1. Clone / unzip the repo, then move into the Airflow project
cd WorkSpace-Airflow

# 2. Copy the example env file and fill in your own secrets
cp .env.example .env
# edit .env: set your own SMTP / DB credentials — never commit real secrets

# 3. (Linux only) set the Airflow UID to match your host user
echo -e "AIRFLOW_UID=$(id -u)" >> .env

# 4. Initialize the metadata database and create the default admin user
docker compose up airflow-init

# 5. Start the full cluster (webserver, scheduler, workers, Redis, Postgres)
docker compose up -d

# 6. Open the UI → http://localhost:8080  (default user/pass: airflow / airflow)
```

> ⚠️ **Security note:** the original `.env` used while recording this course had a live SendGrid API key committed to it. This repo ships a redacted **`.env.example`** instead — copy it to `.env` and put in your own credentials. If you're reusing the original `.env` anywhere, rotate that SendGrid key first.

<details>
<summary><strong>🎬 Everyday usage commands</strong></summary>

```bash
# Tail scheduler logs
docker compose logs -f airflow-scheduler

# Open a shell inside the webserver container
docker compose exec airflow-webserver bash

# List all DAGs known to Airflow
docker compose exec airflow-webserver airflow dags list

# Trigger a DAG manually
docker compose exec airflow-webserver airflow dags trigger 1_1_1_manual_dag

# Stop the cluster (keep volumes/data)
docker compose down

# Stop and wipe everything (fresh start)
docker compose down --volumes --remove-orphans
```

</details>

To follow the course in order, drop the files from `0- My Dags/Airflow - Day 1 Files/Day 1 DAGs` and `Day 2 Files/Day 2 DAGs` into `dags/` one at a time (or all at once) and watch them appear in the UI.

**Browsing the notes:** open the `Obsidian/` folder as a vault in Obsidian, then start from `Apache Airflow/Airflow - Topics.md` — it's the linked index for the whole syllabus.

<div align="right"><a href="#airflow-course">↑ back to top</a></div>

---

## 🗂️ Project Structure

<details open>
<summary><strong>Click to expand / collapse the full tree</strong></summary>

```
.
├── README.md
│
├── WorkSpace-Airflow/
│   ├── dags/                                   # Active DAGs folder mounted into the scheduler
│   ├── plugins/                                # Airflow plugins folder (mounted, empty by default)
│   ├── config/                                 # Airflow config overrides (mounted, empty by default)
│   ├── docker-compose.yaml                     # Official Airflow docker-compose (CeleryExecutor + Redis + Postgres)
│   ├── .env.example                            # Redacted environment template — copy to .env
│   │
│   └── 0- My Dags/
│       ├── Airflow - Day 1 Files/
│       │   ├── Day 1 DAGs/                     # 1-1 → 4-5: definitions, operators, context, variables, params
│       │   ├── Day 1 Labs/                     # Lab briefs (screenshots)
│       │   └── (data/, scripts/)               # CSVs + SQL/Bash/Python helper scripts used by the DAGs
│       └── Airflow - Day 2 Files/
│           ├── Day 2 DAGs/                     # 5-1 → 11-2: sensors, branching, XComs, hooks, pools, SLAs, MongoDB
│           ├── Day 2 Labs/                     # Lab briefs (screenshots)
│           └── (data/, scripts/)
│
└── Obsidian/
    └── Apache Airflow/
        ├── Airflow - Topics.md                 # Course index / table of contents
        ├── Airflow - Excalidraw/               # Architecture & flow diagrams
        ├── Airflow - Images/                   # Screenshots referenced by the notes
        └── *.md                                # One note per concept (19 files)
```

</details>

<div align="right"><a href="#airflow-course">↑ back to top</a></div>

---

## 🗺️ Course Outline

The Obsidian vault (`Obsidian/Apache Airflow/Airflow - Topics.md`) is the single source of truth for the syllabus:

<details open>
<summary><strong>📅 Day 1 — Concepts & Core Development</strong></summary>
<br/>

1. Airflow for Workflow Orchestration
2. Airflow Concepts & Terminology
3. Airflow Core Architecture Components
4. Airflow Web UI vs. CLI
5. Airflow DAGs & Triggers
6. Airflow Core Operators
7. Airflow Providers Operators (SQL Databases)
8. Airflow Variables & Jinja

*+ Simple Data Pipeline walkthrough, Day 1 architecture diagrams*

</details>

<details open>
<summary><strong>📅 Day 2 — Advanced Logic & Modern Data Stack</strong></summary>
<br/>

9. Airflow Sensors
10. Workflow Control (Decisions & Dependencies)
11. Airflow XComs
12. Airflow Hooks & Connections
13. Airflow Pools & Resource Management
14. Airflow SLAs & Alerts
15. Providers Operators (NoSQL)
16. Providers Operators (Data Engineering Stack)
17. Migration from Airflow 2 to Airflow 3

*+ Day 2 architecture diagrams*

</details>

<div align="right"><a href="#airflow-course">↑ back to top</a></div>

---

## 🧭 Roadmap

- [ ] Add a `docker-compose.override.yaml` example for adding Postgres/Mongo as extra services
- [ ] Convert the Obsidian notes into a published static site (MkDocs/Docusaurus)
- [ ] Add a `Makefile` wrapping the common `docker compose` commands

---

## 🤝 Contributing

This is course material shared for learning purposes — issues and PRs pointing out mistakes or outdated syntax (especially around the Airflow 2 → 3 migration notes) are welcome.

---

## 📜 License

Released under the [MIT License](LICENSE).

<div align="right"><a href="#airflow-course">↑ back to top</a></div>

---

<div align="center">

## 👤 Author

<img src="https://github.com/diea-abdeltwab.png" width="90" style="border-radius:50%" alt="Diea Abdeltwab"/>

### **Diea Abdeltwab**
Data Engineer · Software Engineer

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/diea-abdeltwab/)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/diea-abdeltwab)
[![Email](https://img.shields.io/badge/Email-D14836?style=for-the-badge&logo=gmail&logoColor=white)](mailto:dieaabdeltwab2024@gmail.com)

<sub>If this saved you time, a ⭐ on the repo helps others find it.</sub>

[![footer](https://capsule-render.vercel.app/api?type=waving&color=0:111214,50:16324a,100:0F2238&height=110&section=footer)](https://capsule-render.vercel.app)

</div>
