# Paddy Statistics Lakehouse (Sri Lanka Maha & Yala)

End-to-end data engineering project that builds a lakehouse-style pipeline on AWS for Sri Lankan paddy statistics (Maha/Yala seasons: production, harvested extent, sown extent, and yield by district and year).

---

## Architecture (High Level)

- **Data Lake**: S3 (`bronze` → `silver` → `silver_clean` → `gold`)
- **Compute**: AWS Lambda (Python) and Athena (CTAS SQL)
- **Catalog**: AWS Glue Data Catalog / Hive-style partitions
- **Query**: Athena on top of S3 Silver / Silver Clean / Gold layers
- **Output**: District-level and national-level fact tables + trend views

---

## Pipeline Stages

### 1. Ingestion & Cleaning (Silver)

- Raw Excel/CSV files uploaded to `s3://<bucket>/bronze/paddy_stats/`.
- AWS Lambda is triggered or invoked manually with the S3 key.
- Lambda responsibilities:
  - Read file from S3.
  - Detect and flatten multi-row headers.
  - Drop annotation/footer rows.
  - Identify the district column.
  - Melt wide sheets into long (tidy) format.
  - Extract:
    - `metric_name` (Production, Sown Extent, Harvested Extent, Average Yield)
    - `season` (Maha/Yala from filename)
    - `year_label`
    - `subcategory` (MAJOR, MINOR, RAINFED, TOTAL)
    - `harvest_year` (handles ranges like `1978/1979`).
  - Clean numeric values.
  - Write partitioned Parquet to Silver:

    - `silver/paddy_stats/metric_name=<metric_name>/season=<Maha|Yala|unknown>/harvest_year=<YYYY|unknown>/part-<checksum>.parquet`

Silver schema:

- `district`
- `value`
- `year_label`
- `subcategory`
- `metric_name`
- `season`
- `unit`
- `harvest_year`

---

### 2. Normalisation (Silver Clean)

- Implemented via Athena CTAS: `paddy_db.paddy_stats_clean`.
- Reads from `paddy_db.paddy_stats` and writes to:

  - `s3://<bucket>/silver/paddy_stats_clean/`

- Transformations:
  - Standardise district names (trim + uppercase).
  - Fix spelling variants (e.g. `KILINOCHCHI`, `KILLINOCHCHI`, `KILINOCHCHIYA` → `KILINOCHCHI`).
  - Normalise `MAHAWELI 'H'` variants.
  - Remove non-district rows:
    - `SRI LANKA` (national totals)
    - `HIGHLAND PADDY` (product type, not a district)
    - `DISTRICT` header rows.
  - Remove blank districts.
- Partitioned by:
  - `season`
  - `harvest_year`

`paddy_stats_clean` is the authoritative Silver dataset used by Gold.

---

### 3. Gold – District-Level Fact

**Table:** `paddy_db.fact_paddy_district_year_season`  
**Grain:** `district × harvest_year × season`

Built from `paddy_stats_clean` with the following rules:

- Uses only `subcategory = 'TOTAL'` / `'ALL'` where subcategories exist.
- Excludes rows where:
  - `district` is NULL
  - `district` is blank
  - `district = 'SRI LANKA'`
- Columns:
  - `district`
  - `sown_extent_ha`
  - `harvested_extent_ha`
  - `production_mt`
  - `yield_reported_kg_per_ha`
  - `created_at`
  - `harvest_year`
  - `season`
- Partitioned by:
  - `harvest_year`
  - `season`

This table is the main Gold fact for district-level analysis across seasons and years.

---

### 4. Gold – National-Level Fact

**Table:** `paddy_db.fact_paddy_national_year_season`  
**Grain:** `Sri Lanka × harvest_year × season`

Built from `fact_paddy_district_year_season`:

- Aggregates:
  - `national_sown_extent_ha = SUM(sown_extent_ha)`
  - `national_harvested_extent_ha = SUM(harvested_extent_ha)`
  - `national_production_mt = SUM(production_mt)`
- Computes:
  - `national_yield_reported_kg_per_ha` as a **weighted average** of district `yield_reported_kg_per_ha`, weighted by `harvested_extent_ha`.
- Columns:
  - `harvest_year`
  - `season`
  - `national_sown_extent_ha`
  - `national_harvested_extent_ha`
  - `national_production_mt`
  - `national_yield_reported_kg_per_ha`
  - `created_at`
- Partitioned by:
  - `harvest_year`
  - `season`

This table supports national-level Maha vs Yala trend analysis.

---

### 5. Analytical Views (Trends)

**View:** `paddy_db.v_paddy_national_trends`

- Built on top of `fact_paddy_national_year_season`.
- Adds year-on-year metrics using window functions (`LAG`), such as:
  - `production_yoy_pct`
  - `national_yield_yoy_pct`
  - `harvested_extent_yoy_pct`
- Used as a BI-ready layer for tools like Athena, Tableau, or QuickSight.

---

## Summary

This project demonstrates:

- A full lakehouse-style architecture on AWS (S3 + Lambda + Athena + Glue).
- Multi-layer design: Bronze → Silver → Silver Clean → Gold.
- Use of partitioned Parquet on S3 for efficient querying.
- Data cleaning and dimensional standardisation:
  - District normalisation (Kilinochchi, Mahaweli H, removal of non-district rows).
  - Subcategory handling (Major/Minor/Rainfed/Total, with Gold using only TOTAL).
- Construction of both district-level and national-level fact tables.
- Addition of analytical views for year-on-year trend analysis across 40+ years of Sri Lankan paddy statistics.