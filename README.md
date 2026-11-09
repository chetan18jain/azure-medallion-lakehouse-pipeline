# Azure Medallion Lakehouse Pipeline

A personal portfolio project: a Bronze / Silver / Gold (medallion) batch
pipeline built with **PySpark + Delta Lake**, orchestrated by an **Azure
Data Factory** pipeline definition. Local folders stand in for ADLS Gen2
containers, so the whole flow runs on a laptop with no Azure account.

## Architecture

```
raw CSV extracts                ADLS Gen2 (prod) / local folders (demo)
      |                                   |
      v                                   v
  Bronze  -- raw data in Delta, source fidelity, ingested_at added
      |
  Silver  -- cleansed + conformed: dedupe, types, valid statuses,
             data-quality gates must pass or the run fails
      |
  Gold    -- business aggregates: daily_sales, customer_summary
```

- **Bronze** (`src/bronze_ingest.py`) — lands extracts as Delta tables
  unchanged apart from an `ingested_at` timestamp. Nothing is filtered:
  if the source sent it, Bronze keeps it, so any later layer can be
  rebuilt from Bronze alone.
- **Silver** (`src/silver_transform.py`) — the only place business
  cleansing rules live: de-duplication on natural keys, type enforcement,
  trimming, status validation. Quality expectations from
  `src/data_quality.py` (minimum rows, no null keys, no duplicate keys,
  allowed status values) run before anything is written and fail the job
  on a breach.
- **Gold** (`src/gold_aggregate.py`) — reporting-shaped aggregates:
  revenue/orders per day and category, and a per-customer summary.
  Only `COMPLETED` orders count toward revenue.

## Run it locally

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python src/generate_sample_data.py --out data/raw   # sample orders/customers/products
python src/bronze_ingest.py   --raw data/raw     --bronze data/bronze
python src/silver_transform.py --bronze data/bronze --silver data/silver
python src/gold_aggregate.py  --silver data/silver --gold data/gold
python src/maintenance.py     --gold data/gold      # OPTIMIZE + Z-ORDER
```

## Orchestration (ADF)

`adf/pipeline.json` is an Azure Data Factory pipeline that runs the same
jobs as Databricks Spark Python activities, chained
Bronze -> Silver -> Gold -> Maintenance, with folder paths passed as
pipeline parameters. In production the parameters point at ADLS Gen2
containers (`abfss://bronze@<account>.dfs.core.windows.net/...`) and the
notebooks run on Databricks; the job code does not change.

## Design notes

- **Partitioning** — orders and `daily_sales` are partitioned by
  `event_date`, the dominant filter in reporting queries, so scans prune
  whole partitions. Low-cardinality columns (status, category) are
  deliberately not partition columns.
- **Incremental loads** — this demo overwrites per run for clarity; the
  production pattern is a Delta `MERGE` keyed on the natural key with a
  watermark on `event_date` (see the companion
  `airflow-metadata-driven-etl` project for a worked merge), which keeps
  Bronze append-only and Silver/Gold idempotent.
- **Small-file handling** — `gold/daily_sales` is coalesced to one file
  per partition on write, and `src/maintenance.py` runs `OPTIMIZE` to
  compact small files plus `Z-ORDER BY (customer_id)` on
  `customer_summary` so per-customer lookups skip most files.
- **Failure behaviour** — a failed Silver quality gate stops the chain:
  ADF only starts Gold on Silver success, so bad data never reaches the
  reporting layer.

## Layout

```
adf/pipeline.json            ADF orchestration definition
src/generate_sample_data.py  sample CSV generator
src/bronze_ingest.py         raw -> Bronze Delta
src/silver_transform.py      Bronze -> Silver (+ quality gates)
src/gold_aggregate.py        Silver -> Gold aggregates
src/data_quality.py          expectation checks used by the jobs
src/maintenance.py           OPTIMIZE / Z-ORDER upkeep
```
