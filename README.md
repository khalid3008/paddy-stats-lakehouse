# Paddy Statistics Lakehouse (Sri Lanka Maha & Yala)

End-to-end data engineering project that builds a small lakehouse-style pipeline on AWS for Sri Lankan paddy statistics (Maha/Yala seasons, production, yield, and extent by district and year).

## Architecture (High Level)

- **Data Lake**: S3 (`bronze` → `silver` → `gold` folders)
- **Compute**: AWS Lambda (Python) and Athena
- **Catalog**: AWS Glue Data Catalog / Hive-style partitions
- **Query**: Athena on top of S3 silver/gold layers

## Pipeline Stages

1. **Ingestion & Cleaning (Silver)**
   - Input: CSV files from Department of Census & Statistics
   - Logic:
     - Standardise column names (district, season, harvest_year, metrics)
     - Fix inconsistent year formats (e.g. `198` → `1980`)
     - Write partitioned Parquet to S3:  
       `s3://.../silver/paddy_stats/metric_name=<...>/season=<...>/harvest_year=<...>/`

2. **Serving / Analytics (Gold)**
   - Aggregations by district, season, and year
   - Athena views for:
     - District trends over time
     - Maha vs Yala comparisons
     - National production trends

## Repository Layout

- `src/lambda/` — Lambda functions for ingest/transform
- `src/glue/` — (Later) Glue / PySpark jobs for heavier transforms
- `sql/` — Athena DDL and example analysis queries
- `docs/` — Architecture, data model, and runbook
- `samples/` — Tiny, anonymised sample files mirroring source schema

## Key Learnings (for myself)

- Designing **bronze/silver/gold** layers on S3
- Working with **Hive partitions** and **Athena**
- Handling **schema drift & data quality issues** (e.g. malformed years)
- Building a **portfolio-ready DE project** with proper Git version control
