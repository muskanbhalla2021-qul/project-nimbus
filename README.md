# project-nimbus
Cloud-native AWS ETL pipeline: S3 → Glue (PySpark) → Redshift, orchestrated via Apache Airflow with partition pruning, incremental loading, and IAM security.

What is Project Nimbus?
Project Nimbus is a fully automated, cloud-native ETL pipeline built on AWS that takes raw CSV data, transforms it into clean, partitioned Parquet files, loads it into a Redshift data warehouse, and orchestrates the entire flow via Apache Airflow — with zero manual intervention.
Built to mirror real-world data engineering pipelines at scale.

Architecture
Raw CSV Files
     │
     ▼
┌─────────────┐
│  AWS S3     │  ← Raw Zone (landing bucket)
│  (Raw)      │
└──────┬──────┘
       │
       ▼
┌─────────────────────────┐
│  AWS Glue + PySpark     │  ← Transform Layer
│  • Null handling        │     - Schema enforcement
│  • Deduplication        │     - Type casting
│  • Schema validation    │     - Parquet conversion
└──────────┬──────────────┘
           │
           ▼
┌─────────────┐
│  AWS S3     │  ← Curated Zone (clean, partitioned Parquet)
│  (Curated)  │     Partitioned by year / month / day
└──────┬──────┘
       │
       ▼
┌──────────────────┐
│ Amazon Redshift  │  ← Analytics Warehouse
│ COPY + Manifest  │     Incremental loading, no full reloads
└──────────────────┘
       ▲
       │
┌──────────────────┐
│ Apache Airflow   │  ← Orchestration Layer
│ DAGs + Retries   │     Task alerting, SLA monitoring
└──────────────────┘
       ▲
       │
┌──────────────────┐
│   AWS IAM        │  ← Security Layer
│ Least-Privilege  │     Role-based access per pipeline stage
└──────────────────┘

Tech Stack
LayerTechnologyCloud PlatformAWSStorageAmazon S3 (raw + curated zones)TransformationAWS Glue, PySparkWarehouseAmazon RedshiftOrchestrationApache AirflowLanguagePython 3.xFile FormatParquet (snappy compressed)SecurityAWS IAM (least-privilege roles)

Key Features
Multi-Zone S3 Architecture
Data lands in a raw zone untouched, gets transformed, and moves to a curated zone — keeping raw data always recoverable and transformations fully auditable.
PySpark Transformation Layer
Handles null imputation, deduplication, type casting, and schema enforcement at scale — designed to process large volumes without bottlenecks.
Partition Pruning Strategy
S3 data is partitioned by year/month/day — enabling Redshift and Athena to skip irrelevant partitions entirely, drastically reducing query scan costs on time-series data.
Incremental Redshift Loading
Uses COPY commands with manifest files — only new data gets loaded, no expensive full-table reloads.
Airflow DAG Orchestration
Full pipeline is orchestrated as an Airflow DAG with:

Configurable retry logic per task
Task-level failure alerting
SLA breach notifications
Clean dependency management between stages

IAM Security
Every pipeline stage has its own IAM role with least-privilege permissions — no over-permissioned service accounts, no shared credentials.

Project Structure
project-nimbus/
│
├── dags/
│   └── nimbus_pipeline.py       # Airflow DAG definition
│
├── glue_jobs/
│   └── transform.py             # PySpark transformation logic
│
├── redshift/
│   └── load.sql                 # COPY commands + manifest config
│
├── iam/
│   └── policies.json            # IAM role definitions
│
├── data/
│   └── sample_raw.csv           # Sample input data
│
└── README.md

Pipeline Flow
1. Raw CSV lands in S3 (raw zone)
2. Airflow DAG triggers on schedule
3. Glue job spins up PySpark session
4. Transforms: null handling → dedup → type cast → schema validate → Parquet
5. Clean Parquet written to S3 (curated zone, partitioned by date)
6. Redshift COPY loads new partitions incrementally via manifest
7. Airflow marks DAG success / fires alert on failure

Why Parquet?

Columnar storage → queries only scan columns they need
Snappy compression → ~75% smaller than raw CSV
Partition-aware → works seamlessly with Redshift Spectrum and Athena
Schema embedded → no separate schema registry needed for basic pipelines


Setup & Deployment
bash# Clone the repo
git clone https://github.com/muskanbhalla/project-nimbus
cd project-nimbus

# Configure AWS credentials
aws configure

# Deploy Glue job
aws glue create-job --cli-input-json file://glue_jobs/config.json

# Start Airflow
airflow db init
airflow scheduler &
airflow webserver

What I Learned

Designing multi-zone S3 architectures for data lake patterns
Writing production PySpark jobs inside AWS Glue
Partition pruning strategies for cost-optimised querying
Building reliable, retry-safe Airflow DAGs
Applying IAM least-privilege security to pipeline infrastructure
Incremental loading patterns in Redshift using manifest files


Status

🚧 In Progress — Core pipeline functional. Currently adding monitoring layer and expanding transformation logic for multi-source ingestion.


Author -
Muskan Bhalla
Cloud & Data Engineer
muskanbhalla2021@gmail.com
