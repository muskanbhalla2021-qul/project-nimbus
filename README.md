# ☁️ Project Nimbus

### Cloud-native AWS End-to-End ETL Data Pipeline

> *Raw data in. Analytics-ready data out. Fully automated, zero manual intervention.*

---

## What is Project Nimbus?

Project Nimbus is a fully automated, cloud-native ETL pipeline built on AWS. It takes raw CSV data, transforms it into clean partitioned Parquet files using PySpark, loads it into a Redshift data warehouse, and orchestrates the entire flow via Apache Airflow — with zero manual intervention.

Built to mirror real-world data engineering pipelines at scale.

---

## Architecture

```
Raw CSV Files
     |
     v
AWS S3 — Raw Zone (landing bucket)
     |
     v
AWS Glue + PySpark — Transform Layer
     - Null handling
     - Deduplication
     - Type casting
     - Schema validation
     - Parquet conversion
     |
     v
AWS S3 — Curated Zone (clean Parquet, partitioned by year/month/day)
     |
     v
Amazon Redshift — Analytics Warehouse (COPY + Manifest, incremental loading)

Apache Airflow — Orchestration (DAGs, retries, task alerting, SLA monitoring)

AWS IAM — Security (least-privilege roles per pipeline stage)
```

---

## Tech Stack

| Layer | Technology |
| --- | --- |
| Cloud Platform | AWS |
| Storage | Amazon S3 (raw + curated zones) |
| Transformation | AWS Glue + PySpark |
| Warehouse | Amazon Redshift |
| Orchestration | Apache Airflow |
| Language | Python 3.x |
| File Format | Parquet (snappy compressed) |
| Security | AWS IAM (least-privilege roles) |

---

## Key Features

**Multi-Zone S3 Architecture**
Data lands in a raw zone untouched, gets transformed, then moves to a curated zone — raw data is always recoverable and transformations are fully auditable.

**PySpark Transformation Layer**
Handles null imputation, deduplication, type casting, and schema enforcement at scale — designed to process large volumes without bottlenecks.

**Partition Pruning Strategy**
S3 data is partitioned by `year/month/day` — enabling Redshift and Athena to skip irrelevant partitions entirely, drastically reducing query scan costs on time-series data.

**Incremental Redshift Loading**
Uses COPY commands with manifest files — only new data gets loaded, no expensive full-table reloads.

**Airflow DAG Orchestration**
- Configurable retry logic per task
- Task-level failure alerting
- SLA breach notifications
- Clean dependency management between stages

**IAM Security**
Every pipeline stage has its own IAM role with least-privilege permissions — no over-permissioned service accounts, no shared credentials.

---

## Project Structure

```
project-nimbus/
|
|-- dags/
|   |-- nimbus_pipeline.py       # Airflow DAG definition
|
|-- glue_jobs/
|   |-- transform.py             # PySpark transformation logic
|
|-- redshift/
|   |-- load.sql                 # COPY commands + manifest config
|
|-- iam/
|   |-- policies.json            # IAM role definitions
|
|-- data/
|   |-- sample_raw.csv           # Sample input data
|
|-- README.md
```

---

## Pipeline Flow

```
Step 1 — Raw CSV lands in S3 (raw zone)
Step 2 — Airflow DAG triggers on schedule
Step 3 — Glue job spins up PySpark session
Step 4 — Transforms: null handling -> dedup -> type cast -> schema validate -> Parquet
Step 5 — Clean Parquet written to S3 (curated zone, partitioned by date)
Step 6 — Redshift COPY loads new partitions incrementally via manifest
Step 7 — Airflow marks DAG success / fires alert on failure
```

---

## Why Parquet?

| Feature | Benefit |
| --- | --- |
| Columnar storage | Queries only scan the columns they need |
| Snappy compression | ~75% smaller than raw CSV |
| Partition-aware | Works seamlessly with Redshift Spectrum and Athena |
| Schema embedded | No separate schema registry needed |

---

## Setup

```bash
# Clone the repo
git clone https://github.com/muskanbhalla2021-qul/project-nimbus
cd project-nimbus

# Configure AWS credentials
aws configure

# Deploy Glue job
aws glue create-job --cli-input-json file://glue_jobs/config.json

# Start Airflow
airflow db init
airflow scheduler &
airflow webserver
```

---

## What I Learned

- Designing multi-zone S3 architectures for data lake patterns
- Writing production PySpark jobs inside AWS Glue
- Partition pruning strategies for cost-optimised querying
- Building reliable, retry-safe Airflow DAGs
- Applying IAM least-privilege security across pipeline infrastructure
- Incremental loading patterns in Redshift using manifest files

---

## Status

> **In Progress** — Core pipeline functional. Currently adding a monitoring layer and expanding transformation logic for multi-source ingestion.

---

## Author

**Muskan Bhalla** – Cloud & Data Engineer  
muskanbhalla2021@gmail.com  

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue?style=flat&logo=linkedin)](https://www.linkedin.com/in/muskan-bhalla-975a65222/)

[![GitHub](https://img.shields.io/badge/GitHub-Follow-black?style=flat&logo=github)](https://github.com/muskanbhalla2021-qul)
