# Ecommerce Data Engineering Pipeline - Olist

A batch data pipeline that ingests the Olist Brazilian e-commerce dataset, performs data validation, and builds a PostgreSQL-based analytical model (fact and dimension tables) for querying business insights.

## Architecture

```
CSV → Ingestion (Python) → Staging (PostgreSQL)
    → Validation (Python)
    → Transformation (SQL) → Fact & Dimension Tables
```

![Architecture](/images/architecture.jpg)

## Data Model

![DB schema](/images/schema.jpg)

**fact_orders**: one row per order; contains ~112k rows across ~99k orders from September 2016 to October 2018.

## Getting Started

### 1. Clone the repo
```bash
git clone https://github.com/yourname/ecommerce-de-pipeline.git
cd ecommerce-de-pipeline
```

### 2. Download the dataset
Download [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and place all CSVs in `data/raw/`

Required files:
- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_customers_dataset.csv`
- `olist_products_dataset.csv`

### 3. Setup environment and install dependencies
```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### 4. Run pipeline
```bash
python run_pipeline.py
```

## Project structure

```
ecommerce-de-pipeline/
│
├── data/raw/             # input CSVs
│
├── db/
│   └── schema.sql        # staging and warehouse table definitions (DDL)
│
├── images/               # architecture and schema diagram
│
├── src/
│   ├── __init__.py
│   ├── ingest.py         # CSV → staging tables
│   ├── transform.py      # runs transform.sql via psycopg2
│   ├── transform.sql     # all SQL transformation logic (DML)
│   └── utils.py          # shared DB connection
│   ├── validate.py       # data quality checks
│
├── .env
├── .env.example
├── .gitignore
├── README.md
├── requirements.txt
└── run_pipeline.py       # orchestrates all stages
```

## Design decisions

* Staging tables mirror CSV columns exactly with no type conversion and uses all-TEXT columns. Postgres type casting happens in `transform.sql` where bad values can be handled explicitly with `NULLIF` and `TRIM` before casting.
Enforcing types at load would cause silent failures on the known inconsistencies in this dataset.
* SQL handles all transformations, keeps business logic centralized and auditable.
* Dimension tables use SCD Type 1 (overwrite). Olist is a static historical snapshot with no future updates, so SCD Type 2 (start/end dates, active flag) is unnecessary as it would produce columns that are always NULL or always TRUE.
* Full reload (truncate + load): Simpler and reliable for batch pipelines

## Known limitations

* No incremental loading: Since the dataset is static, the pipeline is truncate-and-reload on every run
* Limited dataset coverage: payments/sellers data are not currently ingested
* No orchestration tool (Airflow)

## Tech stack

* Python (pandas, psycopg2)
* PostgreSQL
* SQL