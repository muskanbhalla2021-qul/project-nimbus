☁️ Project Nimbus
Cloud-native AWS End-to-End ETL Data Pipeline

Raw data in. Analytics-ready data out. Fully automated, zero manual intervention.


📌 What is Project Nimbus?
Project Nimbus is a fully automated, cloud-native ETL pipeline built on AWS. It takes raw CSV data, transforms it into clean partitioned Parquet files using PySpark, loads it into a Redshift data warehouse, and orchestrates the entire flow via Apache Airflow.
Built to mirror real-world data engineering pipelines at scale.

🏗️ Architecture
Raw CSV Files
      │
      ▼
┌─────────────────┐
│   AWS S3 (Raw)  │  ← Landing zone
└────────┬────────┘
         │
         ▼
┌──────────────────────────┐
│   AWS Glue + PySpark     │  ← Transform layer
│   - Null handling        │
│   - Deduplication        │
│   - Type casting         │
│   - Schema validation    │
│   - Parquet conversion   │
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────┐
│  AWS S3 (Curated)    │  ← Clean, partitioned Parquet
│  year / month / day  │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│   Amazon Redshift    │  ← Analytics warehouse
│   COPY + Manifest    │     Incremental loading
└──────────────────────┘

┌──────────────────────┐
│   Apache Airflow     │  ← Orchestration
│   DAGs + Retries     │     Task alerting, SLA monitoring
└──────────────────────┘

┌──────────────────────┐
│   AWS IAM            │  ← Security
│   Least-Privilege    │     Role-based access per stage
└──────────────────────┘

🛠️ Tech Stack
LayerTechnologyCloud PlatformAWSStorageAmazon S3 (raw + curated zones)TransformationAWS Glue + PySparkWarehouseAmazon RedshiftOrchestrationApache AirflowLanguagePython 3.xFile FormatParquet (snappy compressed)SecurityAWS IAM (least-privilege roles)

✨ Key Features
🗂️ Multi-Zone S3 Architecture
Data lands in a raw zone untouched, gets transformed, and moves to a curated zone — keeping raw data always recoverable and transformations fully auditable.
⚡ PySpark Transformation Layer
Handles null imputation, deduplication, type casting, and schema enforcement at scale — designed to process large volumes without bottlenecks.
📂 Partition Pruning Strategy
S3 data is partitioned by year/month/day — enabling Redshift and Athena to skip irrelevant partitions entirely, drastically reducing query scan costs on time-series data.
🔄 Incremental Redshift Loading
Uses COPY commands with manifest files — only new data gets loaded, no expensive full-table reloads.
🎛️ Airflow DAG Orchestration
Full pipeline orchestrated as an Airflow DAG with:

Configurable retry logic per task
Task-level failure alerting
SLA breach notifications
Clean dependency management between stages

🔒 IAM Security
Every pipeline stage has its own IAM role with least-privilege permissions — no over-permissioned service accounts, no shared credentials.

📁 Project Structure
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

🔁 Pipeline Flow
1. Raw CSV lands in S3 (raw zone)
2. Airflow DAG triggers on schedule
3. Glue job spins up PySpark session
4. Transforms: null handling → dedup → type cast → schema validate → Parquet
5. Clean Parquet written to S3 (curated zone, partitioned by date)
6. Redshift COPY loads new partitions incrementally via manifest
7. Airflow marks DAG success / fires alert on failure

🤔 Why Parquet?
FeatureBenefitColumnar storageQueries only scan the columns they needSnappy compression~75% smaller than raw CSVPartition-awareWorks seamlessly with Redshift Spectrum and AthenaSchema embeddedNo separate schema registry needed

🚀 Setup & Deployment
bash# Clone the repo
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

📚 What I Learned

Designing multi-zone S3 architectures for data lake patterns
Writing production PySpark jobs inside AWS Glue
Partition pruning strategies for cost-optimised querying
Building reliable, retry-safe Airflow DAGs
Applying IAM least-privilege security across pipeline infrastructure
Incremental loading patterns in Redshift using manifest files


📊 Status

🚧 In Progress — Core pipeline functional. Currently adding monitoring layer and expanding transformation logic for multi-source ingestion.


👩‍💻 Author
Muskan Bhalla — Cloud & Data Engineer
muskanbhalla2021@gmail.com
