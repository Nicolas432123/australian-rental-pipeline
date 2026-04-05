# 🏠 Australian Rental Market Pipeline

An end-to-end data engineering pipeline for the Australian rental market, built with modern open-source tools and deployed on Google Cloud Platform.

---

## 📐 Architecture

```
Raw CSV (GCS)
    ↓
Apache Spark (cleaning & transformation)
    ↓
GCS Data Lake (processed Parquet)
    ↓
BigQuery (australian_rentals dataset)
    ↓
dbt (staging → facts → dimensions → marts)
    ↓
Looker Studio Dashboard
```

All tasks are orchestrated by **Kestra**, running in Docker via sibling container architecture.

---

## 🛠️ Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | Kestra |
| Processing | Apache Spark (PySpark) |
| Data Lake | Google Cloud Storage (GCS) |
| Data Warehouse | BigQuery |
| Transformations | dbt |
| Infrastructure | Terraform |
| Containerization | Docker & Docker Compose |
| Dashboard | Looker Studio |

---

## 📁 Project Structure

```
australian-rental-pipeline/
├── analysis/               # Data validation scripts
│   ├── validate_schema.py
│   ├── validate_business_rules.py
│   └── validate_volume.py
├── credentials/            # GCP credentials (not committed)
│   └── gcp-rentals-sa.json
├── data/
│   ├── raw/                # Raw CSV data
│   └── processed/          # Cleaned Parquet files
├── dbt/
│   └── rentals_dbt/        # dbt project
│       └── models/
│           ├── staging/    # stg_rentals_clean
│           ├── dimensions/ # dim_property_types, dim_suburbs
│           ├── facts/      # fct_rental_listings
│           └── marts/      # mart_suburb_price_summary
├── docker/
│   ├── Dockerfile.spark    # Custom Spark + GCS image
│   └── Dockerfile.dbt      # Custom dbt image
├── kestra/
│   └── flows/
│       └── rentals_pipeline.yml
├── loaders/
│   └── load_rentals_to_bigquery.py
├── spark_jobs/
│   └── clean_rentals.py
├── terraform/              # Infrastructure as Code
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── variables.tf
└── docker-compose.yml
```

---

## ☁️ GCP Infrastructure (Terraform)

The following resources are managed by Terraform:

- **GCS Bucket** — `project-data-talk-club-data-lake` (raw + processed data)
- **GCS Bucket** — `project-data-talk-club-terraform-state` (Terraform remote state)
- **BigQuery Dataset** — `australian_rentals` (raw loaded data)
- **BigQuery Dataset** — `australian_rentals_dbt` (dbt transformed models)

---

## 🚀 Setup & Replication

### Prerequisites

- Docker Desktop installed
- Google Cloud account with a project created
- Terraform installed
- GCP CLI (gcloud) installed

### 1. Clone the repository

```bash
git clone https://github.com/Nicolas432123/australian-rental-pipeline.git
cd australian-rental-pipeline
```

### 2. Set up GCP credentials

1. Go to GCP Console → IAM & Admin → Service Accounts
2. Create a Service Account with the following roles:
   - BigQuery Admin
   - Storage Admin
3. Download the JSON key
4. Save it to `credentials/gcp-rentals-sa.json`

### 3. Provision infrastructure with Terraform

```bash
cd terraform

# Authenticate gcloud
gcloud auth activate-service-account --key-file=../credentials/gcp-rentals-sa.json

# Set credentials for Terraform
export GOOGLE_APPLICATION_CREDENTIALS="../credentials/gcp-rentals-sa.json"

# Initialize and apply
terraform init
terraform apply
```

### 4. Upload raw data to GCS

```bash
cd ..
gsutil cp data/raw/australian_rental_market_2026.csv gs://project-data-talk-club-data-lake/raw/
```

### 5. Build Docker images

```bash
docker build -t rentals-spark -f docker/Dockerfile.spark .
docker build -t rentals-dbt -f docker/Dockerfile.dbt .
```

### 6. Start Kestra

```bash
docker-compose up -d
```

### 7. Run the pipeline

1. Open `http://localhost:8080`
2. Navigate to **Flows** → `australian.rentals` namespace
3. Open `australian_rental_pipeline`
4. Click **Execute**

---

## 🔄 Pipeline Tasks

| Task | Description |
|---|---|
| `spark_cleaning` | Reads raw CSV from GCS, cleans and transforms with PySpark, writes Parquet back to GCS |
| `validate_schema` | Validates column types and nulls |
| `validate_business_rules` | Validates price, bedrooms, parking constraints |
| `validate_volume` | Checks row count reduction between raw and clean |
| `load_bigquery` | Loads cleaned Parquet from GCS into BigQuery |
| `dbt_run` | Runs all dbt models (staging → dimensions → facts → marts) |
| `dbt_test` | Runs all dbt tests |

---

## 📊 dbt Models

```
staging/
  └── stg_rentals_clean       # Cleaned source data

dimensions/
  ├── dim_property_types       # Property type lookup
  └── dim_suburbs              # Suburb lookup

facts/
  └── fct_rental_listings      # Main fact table

marts/
  └── mart_suburb_price_summary  # Avg/min/max price by suburb and state
```

---

## 📈 Dashboard

LINK: https://lookerstudio.google.com/reporting/5dd7d87c-7633-40fd-a2c1-6684920de3ae

The Looker Studio dashboard includes:

- 📊 Average weekly rent by state
- 🥧 Property type distribution
- 📋 Top 10 most expensive suburbs
- 📊 Average rent by number of bedrooms
- 🔢 Scorecards: avg price, max price, total listings

---

## ⚙️ Key Technical Decisions

**Sibling container architecture in Kestra** — Kestra launches task containers as Docker siblings (not children), which means each container needs its own volume mounts and environment variables. The dbt image was built with project files and credentials baked in, and all dbt commands use explicit `cd` calls due to Kestra's per-command shell behavior.

**GCS connector for Spark** — The custom Spark image includes the `gcs-connector` JAR to enable native `gs://` path support in PySpark without local file copies.

**Remote Terraform state** — State is stored in a dedicated GCS bucket to enable team collaboration and prevent state loss.

---

## 🗂️ Dataset

The dataset contains Australian rental listings for 2026, including suburb, state, property type, price, bedrooms, bathrooms, parking spaces, coordinates and agency name.

